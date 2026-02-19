package com.payments.ratelimiter.slot

import com.payments.ratelimiter.config.RateLimitConfig
import com.payments.ratelimiter.config.RateLimitConfigRepository
import com.payments.ratelimiter.db.ScheduledEventSlotTable
import com.payments.ratelimiter.db.WindowCounterTable
import com.payments.ratelimiter.metrics.RateLimiterMetrics
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
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
     * Tracks the furthest window that has been assigned a slot.
     * Updated atomically on every successful assignment.
     * Initialized from DB at startup.
     */
    private val furthestAssignedWindow = AtomicReference<Instant>(Instant.EPOCH)

    /**
     * Initializes `furthestAssignedWindow` from the database to survive restarts.
     * Queries the maximum window_start in rate_limit_window_counter where slot_count > 0.
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

        // Step 3: Compute starting window and dynamic search limit
        var windowStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val headroomDuration = Duration.ofSeconds(headroomWindows.toLong() * config.windowSizeSecs)
        val frontier = furthestAssignedWindow.get()
        val searchLimit = maxOf(frontier, windowStart).plus(headroomDuration)

        // Step 4: Walk through windows
        var windowsSearched = 0
        while (!windowStart.isAfter(searchLimit)) {
            windowsSearched++
            val result = tryClaimSlotInWindow(eventId, windowStart, config)

            when (result) {
                is WindowResult.Assigned -> {
                    metrics.slotsAssigned.increment()
                    // Advance the frontier
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

            // Step 4d: Increment counter
            val newSlotIndex = currentCount
            WindowCounterTable.update({ WindowCounterTable.windowStart eq windowStart }) {
                it[slotCount] = newSlotIndex + 1
            }

            // Step 4e: Compute random jitter within the window
            val jitterMillis = ThreadLocalRandom.current().nextLong(0, config.windowSizeMs)
            val scheduledTime = windowStart.plusMillis(jitterMillis)

            // Step 4f: Insert the slot record
            try {
                val insertedId = ScheduledEventSlotTable.insert {
                    it[ScheduledEventSlotTable.eventId] = eventId
                    it[requestedTime] = Instant.now()
                    it[ScheduledEventSlotTable.windowStart] = windowStart
                    it[ScheduledEventSlotTable.slotIndex] = newSlotIndex
                    it[ScheduledEventSlotTable.scheduledTime] = scheduledTime
                    it[configId] = config.id
                    it[createdAt] = Instant.now()
                } get ScheduledEventSlotTable.id

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
                    // ORA-00001 on event_id unique constraint — concurrent idempotency race
                    metrics.idempotencyRaces.increment()
                    logger.info("Idempotency race for eventId={}, re-reading", eventId)

                    // Revert counter since the slot was not actually consumed
                    WindowCounterTable.update({ WindowCounterTable.windowStart eq windowStart }) {
                        it[slotCount] = newSlotIndex
                    }

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
        return ScheduledEventSlotTable
            .selectAll()
            .where { ScheduledEventSlotTable.eventId eq eventId }
            .firstOrNull()
            ?.let { row ->
                AssignedSlot(
                    slotId = row[ScheduledEventSlotTable.id],
                    eventId = row[ScheduledEventSlotTable.eventId],
                    windowStart = row[ScheduledEventSlotTable.windowStart],
                    slotIndex = row[ScheduledEventSlotTable.slotIndex],
                    scheduledTime = row[ScheduledEventSlotTable.scheduledTime],
                    configId = row[ScheduledEventSlotTable.configId]
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
