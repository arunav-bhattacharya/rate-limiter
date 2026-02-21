package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.config.RateLimitConfigRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.sql.Timestamp
import java.sql.Types
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

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
 * The search limit is computed dynamically within the PL/SQL block: after a skip
 * query finds the first available window, the search limit is set to
 * `first_open_window + headroom`. This is request-scoped — each invocation computes its
 * own limit relative to the requested time's neighborhood, so events at different
 * time horizons never interfere with each other.
 */
@ApplicationScoped
class SlotAssignmentService @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    @param:ConfigProperty(name = "rate-limiter.headroom-windows", defaultValue = "100")
    private val headroomWindows: Int
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentService::class.java)

    /**
     * Assign a scheduling slot for the given event.
     *
     * Executes the entire operation — idempotency check, window walk, lock acquisition,
     * slot insertion, counter update — in a **single JDBC round trip** via an anonymous
     * PL/SQL block. The only Kotlin-side work after the call is interpreting the result.
     *
     * When the requested time falls mid-window, the first window's capacity is
     * proportionally reduced based on remaining time, and jitter is constrained to
     * ensure `scheduledTime >= requestedTime`. Both jitter values and proportional
     * capacity are pre-computed in Kotlin and passed to the PL/SQL block.
     *
     * @param eventId      unique event identifier (idempotency key)
     * @param configName   the rate limit config to use
     * @param requestedTime when the caller wants the event processed
     * @return assigned slot with event ID, scheduled time, and delay from requested time
     * @throws SlotAssignmentException if no window has capacity within the search range
     * @throws ConfigLoadException if no active config exists for the given name
     */
    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        // Step 1: Load config (from 5-second cache)
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(
                configName,
                "No active rate limit config found for: $configName"
            )

        // Step 2: Compute starting window, jitter, and proportional capacity
        val windowStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val elapsedMs = elapsedInWindowMs(windowStart, requestedTime)
        val maxFirstWindow = computeEffectiveMax(config.maxPerWindow, elapsedMs, config.windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, config.windowSizeMs)
        val fullJitterMs = computeFullWindowJitterMs(config.windowSizeMs)
        val headroomSecs = headroomWindows.toLong() * config.windowSizeSecs

        // Step 3: Single round trip — PL/SQL block does everything
        val result = executeSlotAssignment(
            eventId, windowStart, requestedTime, config,
            maxFirstWindow, firstJitterMs, fullJitterMs, headroomSecs
        )

        // Step 4: Interpret result (no DB, pure Kotlin)
        return when (result.status) {
            SlotAssignmentSql.STATUS_NEW -> {
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
                            "${result.windowsSearched} windows"
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
     * Single JDBC round trip: binds 10 IN params, registers 5 OUT params, executes,
     * reads results.
     */
    private fun executeSlotAssignment(
        eventId: String,
        windowStart: Instant,
        requestedTime: Instant,
        config: RateLimitConfig,
        maxFirstWindow: Int,
        firstJitterMs: Long,
        fullJitterMs: Long,
        headroomSecs: Long
    ): SlotAssignmentResult {
        return transaction {
            val rawConnection = this.connection.connection as java.sql.Connection
            rawConnection.prepareCall(SlotAssignmentSql.ASSIGN_SLOT_PLSQL).use { cs ->
                // Bind IN parameters (positions 1-10)
                cs.setString(1, eventId)                              // in_event_id
                cs.setTimestamp(2, Timestamp.from(windowStart))       // in_window_start
                cs.setTimestamp(3, Timestamp.from(requestedTime))     // in_requested_time
                cs.setLong(4, config.configId)                        // in_config_id
                cs.setInt(5, config.maxPerWindow)                     // in_max_per_window
                cs.setLong(6, config.windowSizeSecs)                  // in_window_size_secs
                cs.setInt(7, maxFirstWindow)                          // in_max_first_window
                cs.setLong(8, firstJitterMs)                          // in_first_jitter_ms
                cs.setLong(9, fullJitterMs)                           // in_full_jitter_ms
                cs.setLong(10, headroomSecs)                          // in_headroom_secs

                // Register OUT parameters (positions 11-15)
                cs.registerOutParameter(11, Types.INTEGER)            // ou_status
                cs.registerOutParameter(12, Types.BIGINT)             // ou_slot_id
                cs.registerOutParameter(13, Types.TIMESTAMP)          // ou_scheduled_time
                cs.registerOutParameter(14, Types.TIMESTAMP)          // ou_window_start
                cs.registerOutParameter(15, Types.INTEGER)            // ou_windows_searched

                cs.execute()

                SlotAssignmentResult(
                    status = cs.getInt(11),
                    slotId = cs.getLong(12),
                    scheduledTime = cs.getTimestamp(13)?.toInstant() ?: Instant.EPOCH,
                    windowStart = cs.getTimestamp(14)?.toInstant() ?: Instant.EPOCH,
                    windowsSearched = cs.getInt(15)
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
     * Align a timestamp to the nearest window boundary at or before the given time (floor).
     * Windows are aligned to epoch in multiples of windowSizeSecs.
     *
     * The floor boundary is used as the counter key in rate_limit_window_counter so that
     * all requests landing in the same epoch-aligned window share capacity.
     */
    private fun alignToWindowBoundary(time: Instant, windowSizeSecs: Long): Instant {
        val epochSecond = time.epochSecond
        val alignedEpoch = epochSecond - (epochSecond % windowSizeSecs)
        return Instant.ofEpochSecond(alignedEpoch)
    }

    /**
     * Milliseconds elapsed between the window boundary and the requested time.
     * Returns 0 when requestedTime is exactly on a boundary.
     */
    private fun elapsedInWindowMs(windowStart: Instant, requestedTime: Instant): Long {
        return Duration.between(windowStart, requestedTime).toMillis()
    }

    /**
     * Proportional capacity for the first (partial) window.
     * When requestedTime falls mid-window, capacity is reduced proportionally:
     * `floor(maxPerWindow * remainingMs / windowSizeMs)`.
     * Returns full maxPerWindow when requestedTime is exactly on a boundary.
     */
    private fun computeEffectiveMax(maxPerWindow: Int, elapsedMs: Long, windowSizeMs: Long): Int {
        if (elapsedMs <= 0) return maxPerWindow
        val remainingMs = windowSizeMs - elapsedMs
        return Math.floorDiv(maxPerWindow.toLong() * remainingMs, windowSizeMs).toInt()
    }

    /**
     * Jitter for the first (partial) window.
     * Constrained to `[elapsedMs, windowSizeMs)` to ensure `scheduledTime >= requestedTime`.
     * When elapsedMs is 0 (on-boundary), jitter spans the full window `[0, windowSizeMs)`.
     */
    private fun computeFirstWindowJitterMs(elapsedMs: Long, windowSizeMs: Long): Long {
        val lowerBound = if (elapsedMs > 0) elapsedMs else 0L
        return ThreadLocalRandom.current().nextLong(lowerBound, windowSizeMs)
    }

    /**
     * Jitter for subsequent (full) windows.
     * Spans the entire window range `[0, windowSizeMs)`.
     */
    private fun computeFullWindowJitterMs(windowSizeMs: Long): Long {
        return ThreadLocalRandom.current().nextLong(0, windowSizeMs)
    }
}
