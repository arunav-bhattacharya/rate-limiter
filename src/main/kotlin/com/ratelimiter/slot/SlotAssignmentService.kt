package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.config.RateLimitConfigRepository
import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.metrics.RateLimiterMetrics
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.javatime.JavaInstantColumnType
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

/**
 * Core rate limiting algorithm.
 *
 * Assigns scheduling slots to payment events by walking through time windows,
 * using Oracle `SELECT FOR UPDATE NOWAIT` for pessimistic locking. Threads that
 * lose the lock race skip to the next window instead of blocking, distributing
 * load across windows naturally under concurrent access.
 *
 * The lookahead range is dynamic: it extends to `furthestAssignedWindow + headroom`
 * rather than using a fixed maximum. This self-adjusts to actual load patterns.
 */
@ApplicationScoped
class SlotAssignmentService @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    private val metrics: RateLimiterMetrics,
    @param:ConfigProperty(name = "rate-limiter.headroom-windows", defaultValue = "100")
    private val headroomWindows: Int
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentService::class.java)

    companion object {
        private const val ORA_UNIQUE_CONSTRAINT_VIOLATED = 1   // ORA-00001
        private const val ORA_RESOURCE_BUSY = 54               // ORA-00054
        private const val ORA_DEADLOCK_DETECTED = 60           // ORA-00060
    }

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
     * @param eventId      unique event identifier (idempotency key)
     * @param configName   the rate limit config to use
     * @param requestedTime when the caller wants the event processed
     * @return assigned slot with the scheduled execution time
     * @throws SlotAssignmentException if no window has capacity within the dynamic lookahead range
     * @throws ConfigLoadException if no active config exists for the given name
     */
    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        return metrics.assignmentDuration.recordCallable {
            doAssignSlot(eventId, configName, requestedTime)
        }!!
    }

    private fun doAssignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        // Step 1: Idempotency check
        val existing = findExistingSlot(eventId)
        if (existing != null) {
            metrics.idempotentHits.increment()
            logger.debug("Idempotent hit for eventId={}", eventId)
            return existing
        }

        // Step 2: Load config (from 5-second cache)
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(
                configName,
                "No active rate limit config found for: $configName"
            )

        // Step 3: Compute starting window and dynamic search limit.
        //
        // The search limit is: max(frontier, windowStart) + headroom.
        // - `frontier` (furthestAssignedWindow) is the high-water mark of all
        //   windows ever assigned on this node. Using it ensures that even if
        //   requestedTime maps to a window far in the past, the search range
        //   still extends to where active assignment is happening.
        // - `headroom` is how many windows beyond the frontier we're willing to
        //   scan. This prevents infinite search on sparse data while allowing
        //   the system to absorb bursts that push past the current frontier.
        //
        var windowStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val headroomDuration = Duration.ofSeconds(headroomWindows.toLong() * config.windowSizeSecs)
        val frontier = furthestAssignedWindow.get()

        // Why maxOf(frontier, windowStart)?
        // These two values can diverge in either direction:
        //
        // Case 1 — windowStart behind frontier (common bulk-load scenario):
        //   500K events all target 12:00:00, frontier has advanced to window 4,000.
        //   windowStart snaps to 12:00:00 (window 0). If we used windowStart alone,
        //   searchLimit = 12:00:00 + headroom — only 100 windows, but 0..4,000 are
        //   full. The search would exhaust and fail. Using frontier anchors the limit
        //   to window 4,000 + headroom, reaching the available empty windows.
        //
        // Case 2 — windowStart ahead of frontier (future-targeted event):
        //   System has been filling windows around 12:00:00 (frontier = window 500),
        //   but a new event requests 14:00:00. If we used frontier alone,
        //   searchLimit = 12:00:33 + headroom — far behind 14:00:00, and the loop
        //   wouldn't start. Using windowStart anchors the limit to 14:00:00 + headroom,
        //   letting the event land in the empty windows around its requested time.
        val searchLimit = maxOf(frontier, windowStart).plus(headroomDuration)

        // Step 4: Walk through windows
        var windowsSearched = 0
        while (!windowStart.isAfter(searchLimit)) {
            windowsSearched++
            val result = tryClaimSlotInWindow(eventId, windowStart, config)

            when (result) {
                is WindowResult.Assigned -> {
                    metrics.slotsAssigned.increment()
                    // Advance the frontier atomically. Uses updateAndGet with a
                    // monotonic max to ensure the high-water mark only moves forward,
                    // even under concurrent assignments from multiple threads. This
                    // widens the search limit for all subsequent calls on this node.
                    furthestAssignedWindow.updateAndGet { current ->
                        if (windowStart.isAfter(current)) windowStart else current
                    }
                    return result.slot
                }

                is WindowResult.AlreadyAssigned -> {
                    metrics.idempotentHits.increment()
                    return result.existingSlot
                }

                is WindowResult.Full -> {
                    metrics.windowsFull.increment()
                    logger.debug("Window {} is full, advancing", windowStart)
                }

                is WindowResult.Contended -> {
                    metrics.windowsContended.increment()
                    logger.debug("Window {} is contended, advancing", windowStart)
                }
            }

            windowStart = windowStart.plus(config.windowDuration)
        }

        // All windows within dynamic lookahead exhausted
        metrics.assignmentFailures.increment()
        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = windowsSearched,
            message = "Could not assign slot for event $eventId after searching $windowsSearched windows " +
                    "(frontier=$frontier, searchLimit=$searchLimit)"
        )
    }

    /**
     * Attempt to claim a slot in a specific time window.
     *
     * Runs inside a single database transaction. Uses `SELECT FOR UPDATE NOWAIT`
     * for pessimistic locking — if the row is locked by another transaction,
     * ORA-00054 is caught and converted to [WindowResult.Contended].
     */
    private fun tryClaimSlotInWindow(
        eventId: String,
        windowStart: Instant,
        config: RateLimitConfig
    ): WindowResult {
        return transaction {
            // Step 4a: Ensure counter row exists (plain INSERT, catch ORA-00001 if already exists)
            try {
                exec(
                    "INSERT INTO rate_limit_window_counter (window_start, slot_count) VALUES (?, 0)",
                    listOf(JavaInstantColumnType() to windowStart)
                )
            } catch (e: SQLException) {
                if (oracleErrorCode(e) != ORA_UNIQUE_CONSTRAINT_VIOLATED) throw e
                // Row already exists — this is expected for all but the first event per window
            }

            // Step 4b: SELECT FOR UPDATE NOWAIT on the counter row
            val currentCount: Int
            try {
                currentCount = exec(
                    "SELECT slot_count FROM rate_limit_window_counter WHERE window_start = ? FOR UPDATE NOWAIT",
                    listOf(JavaInstantColumnType() to windowStart)
                ) { rs ->
                    if (rs.next()) rs.getInt("slot_count") else 0
                } ?: 0
            } catch (e: SQLException) {
                return@transaction when (oracleErrorCode(e)) {
                    ORA_RESOURCE_BUSY, ORA_DEADLOCK_DETECTED -> WindowResult.Contended
                    else -> throw e
                }
            }

            // Step 4c: Check capacity
            if (currentCount >= config.maxPerWindow) {
                return@transaction WindowResult.Full
            }

            // Step 4d: Compute random jitter within the window
            val newSlotIndex = currentCount
            val jitterMillis = ThreadLocalRandom.current().nextLong(0, config.windowSizeMs)
            val scheduledTime = windowStart.plusMillis(jitterMillis)

            // Step 4e: Insert the slot record BEFORE incrementing counter.
            // This way, if the INSERT fails (ORA-00001 idempotency race), the counter
            // was never touched — no compensating revert needed.
            try {
                val insertedId = RateLimitEventSlotTable.insert {
                    it[RateLimitEventSlotTable.eventId] = eventId
                    it[requestedTime] = Instant.now()
                    it[RateLimitEventSlotTable.windowStart] = windowStart
                    it[RateLimitEventSlotTable.slotIndex] = newSlotIndex
                    it[RateLimitEventSlotTable.scheduledTime] = scheduledTime
                    it[configId] = config.id
                    it[createdAt] = Instant.now()
                } get RateLimitEventSlotTable.id

                // Step 4f: Increment counter only after slot row is confirmed inserted
                WindowCounterTable.update({ WindowCounterTable.windowStart eq windowStart }) {
                    it[slotCount] = newSlotIndex + 1
                }

                WindowResult.Assigned(
                    AssignedSlot(
                        slotId = insertedId,
                        eventId = eventId,
                        windowStart = windowStart,
                        slotIndex = newSlotIndex,
                        scheduledTime = scheduledTime,
                        configId = config.id
                    )
                )
            } catch (e: SQLException) {
                if (oracleErrorCode(e) == ORA_UNIQUE_CONSTRAINT_VIOLATED) {
                    // ORA-00001 on event_id unique constraint — concurrent idempotency race.
                    // Counter was never incremented, so no revert needed.
                    metrics.idempotencyRaces.increment()
                    logger.info("Idempotency race for eventId={}, re-reading", eventId)

                    val existingSlot = findExistingSlotInTransaction(eventId)
                    if (existingSlot != null) {
                        WindowResult.AlreadyAssigned(existingSlot)
                    } else {
                        throw IllegalStateException(
                            "ORA-00001 on event_id=$eventId but no existing slot found"
                        )
                    }
                } else {
                    throw e
                }
            }
        }
    }

    /**
     * Look up an existing slot by event_id (separate transaction for idempotency check).
     */
    private fun findExistingSlot(eventId: String): AssignedSlot? {
        return transaction {
            findExistingSlotInTransaction(eventId)
        }
    }

    /**
     * Look up an existing slot by event_id within an existing transaction.
     */
    private fun findExistingSlotInTransaction(eventId: String): AssignedSlot? {
        return RateLimitEventSlotTable
            .selectAll()
            .where { RateLimitEventSlotTable.eventId eq eventId }
            .firstOrNull()
            ?.let { row ->
                AssignedSlot(
                    slotId = row[RateLimitEventSlotTable.id],
                    eventId = row[RateLimitEventSlotTable.eventId],
                    windowStart = row[RateLimitEventSlotTable.windowStart],
                    slotIndex = row[RateLimitEventSlotTable.slotIndex],
                    scheduledTime = row[RateLimitEventSlotTable.scheduledTime],
                    configId = row[RateLimitEventSlotTable.configId]
                )
            }
    }

    /**
     * Align a timestamp to the nearest window boundary at or before the given time.
     * Windows are aligned to epoch in multiples of windowSizeSecs.
     */
    private fun alignToWindowBoundary(time: Instant, windowSizeSecs: Int): Instant {
        val epochSecond = time.epochSecond
        val alignedEpoch = epochSecond - (epochSecond % windowSizeSecs)
        return Instant.ofEpochSecond(alignedEpoch)
    }

    /**
     * Extract the Oracle error code from an SQLException, traversing the cause chain.
     */
    private fun oracleErrorCode(e: SQLException): Int {
        var current: Throwable? = e
        while (current != null) {
            if (current is SQLException) {
                val code = current.errorCode
                if (code != 0) return code
            }
            current = current.cause
        }
        return 0
    }
}
