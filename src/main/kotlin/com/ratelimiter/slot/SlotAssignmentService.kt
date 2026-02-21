package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.config.RateLimitConfigRepository
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.sql.Timestamp
import java.sql.Types
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * Core rate limiting algorithm.
 *
 * Assigns scheduling slots to payment events by walking through time windows.
 * The entire slot assignment — idempotency check, window walk loop, lock acquisition,
 * slot insertion, and counter update — executes as a single anonymous PL/SQL block
 * in one JDBC round trip. This minimizes network latency and lock hold time.
 *
 * The PL/SQL block uses Oracle `SELECT FOR UPDATE NOWAIT` for pessimistic locking.
 * Threads that lose the lock race skip to the next window instead of blocking,
 * distributing load across windows naturally under concurrent access.
 *
 * The lookahead range is dynamic: it extends to `furthestAssignedWindow + headroom`
 * rather than using a fixed maximum. This self-adjusts to actual load patterns.
 */
@ApplicationScoped
class SlotAssignmentService @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    @param:ConfigProperty(name = "rate-limiter.headroom-windows", defaultValue = "100")
    private val headroomWindows: Int
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentService::class.java)

    /**
     * High-water mark: the latest (furthest into the future) window that has ever
     * had a slot assigned across any thread on this node.
     *
     * This drives the **dynamic lookahead** — instead of a fixed MAX_LOOKAHEAD, the
     * search limit is computed as:
     *     searchLimit = max(furthestAssignedWindow, requestedWindowStart) + headroom
     *
     * Why this matters:
     * - Eliminates a static ceiling that must be tuned. The frontier advances
     *   organically with load — after 5,000 windows are filled, the search range
     *   is 5,000 + headroom, not a fixed 200.
     * - Prevents unbounded scans on sparse data: headroom caps how far past the
     *   frontier any single request will search (default 100 windows).
     * - Each node maintains its own AtomicReference. Inter-node skew is safe because
     *   the counter table is the source of truth for capacity; this field only bounds
     *   how far the node is willing to look. Nodes re-sync from DB on restart.
     *
     * Lifecycle:
     * - Initialized from DB at startup via [initFurthestWindow] (survives restarts).
     * - Advanced atomically on every successful slot assignment (never decremented).
     */
    private val furthestAssignedWindow = AtomicReference<Instant>(Instant.EPOCH)

    /**
     * Initializes [furthestAssignedWindow] from the database so the dynamic lookahead
     * survives application restarts.
     *
     * Without this, a freshly started node would begin with frontier = EPOCH, meaning
     * its search limit would be `requestedWindowStart + headroom` — potentially much
     * tighter than the actual frontier across the cluster. Events that should land in
     * windows far ahead (because earlier windows are full) could fail with
     * [SlotAssignmentException] until the node "catches up" by assigning enough slots.
     *
     * By reading `MAX(window_start) WHERE slot_count > 0`, we restore the frontier to
     * the latest window that any node has ever written to, giving this node the same
     * search range as nodes that were running continuously.
     */
    @PostConstruct
    fun initFurthestWindow() {
        try {
            val maxWindow = transaction {
                exec(
                    "SELECT MAX(window_start) AS max_ws FROM rate_limit_window_counter WHERE slot_count > 0",
                    emptyList()
                ) { rs ->
                    if (rs.next()) rs.getTimestamp("max_ws")?.toInstant() else null
                }
            }
            if (maxWindow != null) {
                furthestAssignedWindow.set(maxWindow)
                logger.info("Initialized furthestAssignedWindow from DB: {}", maxWindow)
            } else {
                logger.info("No existing window counters found, furthestAssignedWindow starts at EPOCH")
            }
        } catch (e: Exception) {
            logger.warn("Failed to initialize furthestAssignedWindow from DB, starting at EPOCH", e)
        }
    }

    /**
     * Assign a scheduling slot for the given event.
     *
     * Executes the entire operation — idempotency check, window walk, lock acquisition,
     * slot insertion, counter update — in a **single JDBC round trip** via an anonymous
     * PL/SQL block. The only Kotlin-side work after the call is interpreting the result
     * and advancing the frontier.
     *
     * @param eventId      unique event identifier (idempotency key)
     * @param configName   the rate limit config to use
     * @param requestedTime when the caller wants the event processed
     * @return assigned slot with event ID, scheduled time, and delay from requested time
     * @throws SlotAssignmentException if no window has capacity within the dynamic lookahead range
     * @throws ConfigLoadException if no active config exists for the given name
     */
    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        // Step 1: Load config (from 5-second cache)
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(
                configName,
                "No active rate limit config found for: $configName"
            )

        // Step 2: Compute starting window and dynamic search limit
        val windowStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val frontier = furthestAssignedWindow.get()
        val headroomDuration = Duration.ofSeconds(headroomWindows.toLong() * config.windowSizeSecs)
        val searchLimit = maxOf(frontier, windowStart).plus(headroomDuration)

        // Step 3: Single round trip — PL/SQL block does everything
        val result = executeSlotAssignment(eventId, windowStart, searchLimit, requestedTime, config)

        // Step 4: Interpret result (no DB, pure Kotlin)
        return when (result.status) {
            SlotAssignmentSql.STATUS_NEW -> {
                // Advance the frontier atomically (monotonic max)
                furthestAssignedWindow.updateAndGet { current ->
                    if (result.windowStart.isAfter(current)) result.windowStart else current
                }
                logger.info(
                    "Assigned slot for eventId={} in window={} after searching {} windows",
                    eventId, result.windowStart, result.windowsSearched
                )
                buildAssignedSlot(eventId, result.scheduledTime, requestedTime)
            }

            SlotAssignmentSql.STATUS_EXISTING -> {
                logger.debug("Idempotent hit for eventId={}", eventId)
                buildAssignedSlot(eventId, result.scheduledTime, requestedTime)
            }

            SlotAssignmentSql.STATUS_EXHAUSTED -> {
                throw SlotAssignmentException(
                    eventId = eventId,
                    windowsSearched = result.windowsSearched,
                    message = "Could not assign slot for event $eventId after searching " +
                            "${result.windowsSearched} windows (frontier=$frontier, searchLimit=$searchLimit)"
                )
            }

            else -> error("Unexpected PL/SQL status: ${result.status}")
        }
    }

    /**
     * Marshalling result from the PL/SQL block's OUT parameters.
     */
    private data class SlotAssignmentResult(
        val status: Int,
        val slotId: Long,
        val scheduledTime: Instant,
        val windowStart: Instant,
        val windowsSearched: Int
    )

    /**
     * Execute the PL/SQL slot assignment block via CallableStatement.
     * Single JDBC round trip: binds 8 IN params, registers 5 OUT params, executes,
     * reads results.
     */
    private fun executeSlotAssignment(
        eventId: String,
        windowStart: Instant,
        searchLimit: Instant,
        requestedTime: Instant,
        config: RateLimitConfig
    ): SlotAssignmentResult {
        return transaction {
            val rawConnection = this.connection.connection as java.sql.Connection
            rawConnection.prepareCall(SlotAssignmentSql.ASSIGN_SLOT_PLSQL).use { cs ->
                // Bind IN parameters (positions 1-8)
                cs.setString(1, eventId)                              // in_event_id
                cs.setTimestamp(2, Timestamp.from(windowStart))       // in_window_start
                cs.setTimestamp(3, Timestamp.from(searchLimit))       // in_search_limit
                cs.setTimestamp(4, Timestamp.from(requestedTime))     // in_requested_time
                cs.setLong(5, config.configId)                        // in_config_id
                cs.setInt(6, config.maxPerWindow)                     // in_max_per_window
                cs.setLong(7, config.windowSizeSecs)                  // in_window_size_secs
                cs.setLong(8, config.windowSizeMs)                    // in_window_size_ms

                // Register OUT parameters (positions 9-13)
                cs.registerOutParameter(9, Types.INTEGER)             // ou_status
                cs.registerOutParameter(10, Types.BIGINT)             // ou_slot_id
                cs.registerOutParameter(11, Types.TIMESTAMP)          // ou_scheduled_time
                cs.registerOutParameter(12, Types.TIMESTAMP)          // ou_window_start
                cs.registerOutParameter(13, Types.INTEGER)            // ou_windows_searched

                cs.execute()

                SlotAssignmentResult(
                    status = cs.getInt(9),
                    slotId = cs.getLong(10),
                    scheduledTime = cs.getTimestamp(11)?.toInstant() ?: Instant.EPOCH,
                    windowStart = cs.getTimestamp(12)?.toInstant() ?: Instant.EPOCH,
                    windowsSearched = cs.getInt(13)
                )
            }
        }
    }

    /**
     * Build the public [AssignedSlot] from PL/SQL results, computing the delay.
     */
    private fun buildAssignedSlot(
        eventId: String,
        scheduledTime: Instant,
        requestedTime: Instant
    ): AssignedSlot {
        val delay = Duration.between(requestedTime, scheduledTime).let {
            if (it.isNegative) Duration.ZERO else it
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }

    /**
     * Align a timestamp to the nearest window boundary at or before the given time.
     * Windows are aligned to epoch in multiples of windowSizeSecs.
     */
    private fun alignToWindowBoundary(time: Instant, windowSizeSecs: Long): Instant {
        val epochSecond = time.epochSecond
        val alignedEpoch = epochSecond - (epochSecond % windowSizeSecs)
        return Instant.ofEpochSecond(alignedEpoch)
    }
}
