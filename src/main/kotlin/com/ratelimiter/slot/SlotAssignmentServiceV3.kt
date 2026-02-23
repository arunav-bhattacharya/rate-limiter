package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.repo.RateLimitConfigRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.IntegerColumnType
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.javatime.JavaInstantColumnType
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.statements.StatementType
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V3 slot assignment — Kotlin/Exposed implementation.
 *
 * Combines V2's combined find+lock pattern with V1's correctness guarantees:
 * epoch-aligned windows, proportional first-window capacity, dynamic config,
 * and multi-chunk search. Pre-provisions windows in chunks (guarded by an
 * existence check) then uses a single `SELECT ... FOR UPDATE SKIP LOCKED
 * FETCH FIRST 1 ROW ONLY` per chunk instead of V1's nested window-walk loop.
 *
 * All DB work runs in a single transaction. Uses raw SQL for lock queries
 * (Exposed DSL doesn't support FOR UPDATE SKIP LOCKED) and Exposed DSL for
 * inserts and updates.
 */
@ApplicationScoped
class SlotAssignmentServiceV3 @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    @param:ConfigProperty(name = "rate-limiter.max-windows-in-chunk", defaultValue = "100")
    private val maxWindowsInChunk: Int,
    @param:ConfigProperty(name = "rate-limiter.max-search-chunks", defaultValue = "10")
    private val maxSearchChunks: Int
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentServiceV3::class.java)

    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")

        val alignedStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val elapsedMs = Duration.between(alignedStart, requestedTime).toMillis()
        val maxFirstWindow = computeEffectiveMax(config.maxPerWindow, elapsedMs, config.windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, config.windowSizeMs)

        return transaction {
            // 1. Idempotency check
            checkExistingSlot(eventId)?.let { return@transaction it }

            // 2. Phase 1: First window (proportional capacity)
            ensureWindowExists(alignedStart)
            val firstCount = tryLockFirstWindow(alignedStart)
            if (firstCount >= 0 && firstCount < maxFirstWindow) {
                return@transaction claimSlot(
                    eventId, alignedStart, firstJitterMs, requestedTime, config.configId
                )
            }

            // 3. Phase 2: Chunked find+lock
            val windowSize = config.windowSize
            var searchFrom = alignedStart.plus(windowSize)

            for (chunkIdx in 0 until maxSearchChunks) {
                val chunkEnd = searchFrom.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))

                ensureChunkProvisioned(searchFrom, maxWindowsInChunk, windowSize)

                val foundWindow = findAndLockFirstAvailable(searchFrom, chunkEnd, config.maxPerWindow)
                if (foundWindow != null) {
                    val jitterMs = computeFullWindowJitterMs(config.windowSizeMs)
                    logger.info(
                        "Assigned slot for eventId={} in window={} (chunk {})",
                        eventId, foundWindow, chunkIdx
                    )
                    return@transaction claimSlot(
                        eventId, foundWindow, jitterMs, requestedTime, config.configId
                    )
                }

                searchFrom = chunkEnd
            }

            throw SlotAssignmentException(
                eventId = eventId,
                windowsSearched = maxSearchChunks * maxWindowsInChunk,
                message = "Could not assign slot for event $eventId after searching " +
                        "${maxSearchChunks * maxWindowsInChunk} windows across $maxSearchChunks chunks"
            )
        }
    }

    // ---- Transaction helpers ----

    private fun Transaction.checkExistingSlot(eventId: String): AssignedSlot? {
        return RateLimitEventSlotTable
            .selectAll()
            .where { RateLimitEventSlotTable.eventId eq eventId }
            .firstOrNull()
            ?.let { row ->
                val scheduledTime = row[RateLimitEventSlotTable.scheduledTime]
                val reqTime = row[RateLimitEventSlotTable.requestedTime]
                val delay = Duration.between(reqTime, scheduledTime).let { d ->
                    if (d.isNegative) Duration.ZERO else d
                }
                AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
            }
    }

    private fun Transaction.ensureWindowExists(window: Instant) {
        try {
            WindowCounterTable.insert {
                it[windowStart] = window
                it[slotCount] = 0
            }
        } catch (_: ExposedSQLException) {
            // Duplicate key — window already exists
        }
    }

    /**
     * Batch-provision counter rows for a chunk of windows.
     * Guarded by an existence check on the last window — if it exists,
     * the entire chunk is already provisioned and we skip the batch.
     */
    private fun Transaction.ensureChunkProvisioned(
        from: Instant,
        windowCount: Int,
        windowSize: Duration
    ) {
        val lastWindow = from.plus(windowSize.multipliedBy((windowCount - 1).toLong()))
        val exists = WindowCounterTable
            .selectAll()
            .where { WindowCounterTable.windowStart eq lastWindow }
            .count() > 0
        if (exists) return

        val windows = (0 until windowCount).map { i ->
            from.plus(windowSize.multipliedBy(i.toLong()))
        }
        WindowCounterTable.batchInsert(windows, ignore = true, shouldReturnGeneratedValues = false) { window ->
            this[WindowCounterTable.windowStart] = window
            this[WindowCounterTable.slotCount] = 0
        }
    }

    /**
     * Lock the first window's counter row via SELECT FOR UPDATE SKIP LOCKED.
     * Returns slot_count on success, -1 if the row is locked by another session.
     */
    private fun Transaction.tryLockFirstWindow(window: Instant): Int {
        val sql = """
            SELECT SLOT_COUNT
            FROM   rate_limit_window_counter
            WHERE  WINDOW_START = ?
            FOR UPDATE SKIP LOCKED
        """.trimIndent()

        return exec(
            sql,
            listOf(Pair(JavaInstantColumnType(), window)),
            StatementType.SELECT
        ) { rs ->
            if (rs.next()) rs.getInt("SLOT_COUNT") else -1
        } ?: -1
    }

    /**
     * Combined find+lock: atomically finds the earliest non-full, non-contended
     * window in [from, to) and acquires a row lock on it.
     * Returns the window_start, or null if no available window was found.
     */
    private fun Transaction.findAndLockFirstAvailable(
        from: Instant,
        to: Instant,
        maxSlots: Int
    ): Instant? {
        val sql = """
            SELECT WINDOW_START
            FROM   rate_limit_window_counter
            WHERE  WINDOW_START >= ?
            AND    WINDOW_START < ?
            AND    SLOT_COUNT < ?
            ORDER BY WINDOW_START ASC
            FETCH FIRST 1 ROW ONLY
            FOR UPDATE SKIP LOCKED
        """.trimIndent()

        return exec(
            sql,
            listOf(
                Pair(JavaInstantColumnType(), from),
                Pair(JavaInstantColumnType(), to),
                Pair(IntegerColumnType(), maxSlots)
            ),
            StatementType.SELECT
        ) { rs ->
            if (rs.next()) rs.getTimestamp("WINDOW_START").toInstant() else null
        }
    }

    /**
     * Insert event slot + increment window counter. Handles duplicate event_id
     * via catch (idempotency: re-reads existing slot without touching the counter).
     */
    private fun Transaction.claimSlot(
        eventId: String,
        window: Instant,
        jitterMs: Long,
        requestedTime: Instant,
        configId: Long
    ): AssignedSlot {
        val scheduledTime = window.plusMillis(jitterMs)

        val inserted = try {
            RateLimitEventSlotTable.insert {
                it[RateLimitEventSlotTable.eventId] = eventId
                it[RateLimitEventSlotTable.requestedTime] = requestedTime
                it[RateLimitEventSlotTable.windowStart] = window
                it[RateLimitEventSlotTable.scheduledTime] = scheduledTime
                it[RateLimitEventSlotTable.configId] = configId
            }
            true
        } catch (_: ExposedSQLException) {
            false
        }

        if (!inserted) {
            return checkExistingSlot(eventId)
                ?: error("Failed to re-read slot for eventId=$eventId after duplicate key")
        }

        WindowCounterTable.update({ WindowCounterTable.windowStart eq window }) {
            with(SqlExpressionBuilder) {
                it[slotCount] = slotCount + 1
            }
        }

        val delay = Duration.between(requestedTime, scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }

    // ---- Pure computation helpers (no DB) ----

    private fun alignToWindowBoundary(time: Instant, windowSizeSecs: Long): Instant {
        val epochSecond = time.epochSecond
        val alignedEpoch = epochSecond - (epochSecond % windowSizeSecs)
        return Instant.ofEpochSecond(alignedEpoch)
    }

    private fun computeEffectiveMax(maxPerWindow: Int, elapsedMs: Long, windowSizeMs: Long): Int {
        if (elapsedMs <= 0) return maxPerWindow
        val remainingMs = windowSizeMs - elapsedMs
        return Math.floorDiv(maxPerWindow.toLong() * remainingMs, windowSizeMs).toInt()
    }

    private fun computeFirstWindowJitterMs(elapsedMs: Long, windowSizeMs: Long): Long {
        val lowerBound = if (elapsedMs > 0) elapsedMs else 0L
        return ThreadLocalRandom.current().nextLong(lowerBound, windowSizeMs)
    }

    private fun computeFullWindowJitterMs(windowSizeMs: Long): Long {
        return ThreadLocalRandom.current().nextLong(0, windowSizeMs)
    }
}
