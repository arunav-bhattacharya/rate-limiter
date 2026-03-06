package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
import com.ratelimiter.repo.WindowEndTrackerRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Transaction
import java.sql.BatchUpdateException
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadLocalRandom

/**
 * V3 slot assignment — Kotlin/Exposed implementation.
 *
 * Combines V2's combined find+lock pattern with V1's correctness guarantees:
 * epoch-aligned windows, proportional first-window capacity, dynamic config,
 * and frontier-tracked search with configurable extension.
 *
 * Phase 2 uses `track_window_end` (append-only) to track the provisioning frontier
 * per alignedStart. A single find+lock over the entire provisioned range replaces
 * scanning chunk-by-chunk from chunk 0. When the range is full, a configurable
 * extension loop (`max-chunks-to-search`, default 2) provisions new chunks from
 * the frontier. Client retries naturally extend further.
 *
 * DB work is split across multiple short-lived transactions to minimize
 * connection hold time and reduce pool contention under high TPS:
 * - Phase 0 (idempotency check) runs in its own transaction
 * - Chunk provisioning runs in a separate transaction (idempotent batch inserts)
 * - Find+lock+claim runs in a focused transaction (holds row lock briefly)
 *
 * Uses raw SQL for lock queries (Exposed DSL doesn't support FOR UPDATE
 * SKIP LOCKED) and Exposed DSL for inserts and updates.
 */
@ApplicationScoped
class SlotAssignmentServiceV3 @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    private val windowEndTrackerRepository: WindowEndTrackerRepository,
    @param:ConfigProperty(name = "rate-limiter.max-windows-in-chunk", defaultValue = "100")
    private val maxWindowsInChunk: Int,
    @param:ConfigProperty(name = "rate-limiter.max-chunks-to-search", defaultValue = "2")
    private val maxChunksToSearch: Int
) {
    private val firstWindowFull = ConcurrentHashMap<Instant, Boolean>()

    fun evictFirstWindowCache() = firstWindowFull.clear()

    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {

        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")

        val alignedStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val elapsedMs = Duration.between(alignedStart, requestedTime).toMillis()
        val maxFirstWindow = computeEffectiveMax(config.maxPerWindow, elapsedMs, config.windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, config.windowSizeMs)

        // Phase 0: Pre-transaction idempotency check (own short-lived transaction).
        // Releases connection in ~1ms, avoids holding a connection through Phases 1-3
        // for duplicate/retry requests.
        val existing = eventSlotRepository.fetchAssignedSlot(eventId)
        if (existing != null) {
            return existing
        }

        // Phase 1: First window (proportional capacity)
        if (!firstWindowFull.containsKey(alignedStart)) {
            val firstWindowSlot = transaction {
                with(windowSlotCounterRepository) { ensureWindowExists(alignedStart) }

                val lockResult = with(windowSlotCounterRepository) { tryLockFirstWindow(alignedStart, maxFirstWindow) }

                when (lockResult) {
                    true -> {
                        val slot = claimSlot(eventId, alignedStart, firstJitterMs, requestedTime, config.configId)
                        slot
                    }

                    false -> {
                        firstWindowFull[alignedStart] = true; null
                    }

                    null -> null // SKIP LOCKED — contended, try again next request
                }
            }
            if (firstWindowSlot != null) return firstWindowSlot
        }

        // Phase 2: Frontier-tracked find+lock (provisioning and locking in separate transactions)
        val windowSize = config.windowSize

        val windowEnd = windowEndTrackerRepository.fetchWindowEnd(alignedStart)
            ?: provisionInitialRange(alignedStart, windowSize)

        val foundInRange = findLockAndClaim(
            eventId, alignedStart.plus(windowSize), windowEnd,
            config.maxPerWindow, windowSize, requestedTime, config.configId
        )
        if (foundInRange != null) {
            return foundInRange
        }

        // Phase 3: Extension loop — provision chunks and find+lock+claim in separate transactions
        var searchFrom = windowEnd
        for (chunk in 0 until maxChunksToSearch) {
            val chunkEnd = searchFrom.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))

            provisionChunk(searchFrom, maxWindowsInChunk, windowSize, alignedStart, chunkEnd)

            val foundWindow = findLockAndClaim(
                eventId, searchFrom, chunkEnd,
                config.maxPerWindow, windowSize, requestedTime, config.configId
            )

            if (foundWindow != null) {
                return foundWindow
            }

            searchFrom = chunkEnd
        }

        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = maxWindowsInChunk + (maxChunksToSearch * maxWindowsInChunk),
            message = "Could not assign slot for event $eventId after searching " +
                    "initial range + $maxChunksToSearch extension chunks"
        )
    }

    private fun nanosToMs(nanos: Long): String = "%.3f".format(nanos / 1_000_000.0)

    // ---- Split-transaction helpers ----
    // Each helper runs its own short-lived transaction to minimize connection hold time.

    /**
     * Provision the initial range of windows and record the frontier.
     * Runs in its own transaction (idempotent batch inserts + frontier insert).
     */
    private fun provisionInitialRange(alignedStart: Instant, windowSize: Duration): Instant {
        val chunkStart = alignedStart.plus(windowSize)
        val windowEnd = chunkStart.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))
        transaction {
            ensureChunkProvisioned(chunkStart, maxWindowsInChunk, windowSize)
            try {
                with(windowEndTrackerRepository) {
                    insertWindowEnd(alignedStart, windowEnd)
                }
            } catch (_: ExposedSQLException) {
                // Concurrent thread already inserted this frontier — safe to ignore
            } catch (_: BatchUpdateException) {
                // Concurrent thread already inserted this frontier — safe to ignore
            }
        }
        return windowEnd
    }

    /**
     * Provision a chunk of windows and record the frontier.
     * Runs in its own transaction (idempotent batch inserts + frontier insert).
     */
    private fun provisionChunk(
        from: Instant,
        windowCount: Int,
        windowSize: Duration,
        alignedStart: Instant,
        chunkEnd: Instant
    ) {
        transaction {
            ensureChunkProvisioned(from, windowCount, windowSize)
            try {
                with(windowEndTrackerRepository) {
                    insertWindowEnd(alignedStart, chunkEnd)
                }
            } catch (_: ExposedSQLException) {
                // Concurrent thread already inserted this frontier — safe to ignore
            } catch (_: BatchUpdateException) {
                // Concurrent thread already inserted this frontier — safe to ignore
            }
        }
    }

    /**
     * Find an available window, lock it, and claim the slot — all in one short transaction.
     * Returns null if no available window found in [from, to).
     */
    private fun findLockAndClaim(
        eventId: String,
        from: Instant,
        to: Instant,
        maxSlots: Int,
        windowSize: Duration,
        requestedTime: Instant,
        configId: String
    ): AssignedSlot? {
        return transaction {
            val found = with(windowSlotCounterRepository) {
                findAndLockFirstAvailableWindow(from, to, maxSlots, windowSize)
            } ?: return@transaction null

            val jitterMs = computeFullWindowJitterMs(windowSize.toMillis())
            claimSlot(eventId, found, jitterMs, requestedTime, configId)
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
        val exists = with(windowSlotCounterRepository) { windowExists(lastWindow) }
        if (exists) return

        val windows = (0 until windowCount).map { i ->
            from.plus(windowSize.multipliedBy(i.toLong()))
        }
        try {
            with(windowSlotCounterRepository) { batchInsertWindows(windows) }
        } catch (_: ExposedSQLException) {
            // Concurrent thread already provisioned some/all rows — safe to ignore
        } catch (_: BatchUpdateException) {
            // Concurrent thread already provisioned some/all rows — safe to ignore
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
        configId: String
    ): AssignedSlot {
        val scheduledTime = window.plusMillis(jitterMs)

        val inserted = with(eventSlotRepository) {
            insertEventSlot(eventId, requestedTime, window, scheduledTime, configId)
        }

        if (!inserted) {
            return with(eventSlotRepository) { queryAssignedSlot(eventId) }
                ?: error("Failed to re-read slot for eventId=$eventId after duplicate key")
        }

        with(windowSlotCounterRepository) { incrementSlotCount(window) }

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
