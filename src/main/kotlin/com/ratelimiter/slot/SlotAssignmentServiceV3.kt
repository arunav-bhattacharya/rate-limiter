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
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
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
 * All DB work runs in a single transaction. Uses raw SQL for lock queries
 * (Exposed DSL doesn't support FOR UPDATE SKIP LOCKED) and Exposed DSL for
 * inserts and updates.
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
    private val logger = LoggerFactory.getLogger(SlotAssignmentServiceV3::class.java)

    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")

        val alignedStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val elapsedMs = Duration.between(alignedStart, requestedTime).toMillis()
        val maxFirstWindow = computeEffectiveMax(config.maxPerWindow, elapsedMs, config.windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, config.windowSizeMs)

        return transaction {
            // Phase 0: Idempotency check
            with(eventSlotRepository) { queryAssignedSlot(eventId) }?.let { return@transaction it }

            // Phase 1: First window (proportional capacity)
            with(windowSlotCounterRepository) { ensureWindowExists(alignedStart) }
            val firstWindowLocked = with(windowSlotCounterRepository) { tryLockFirstWindow(alignedStart, maxFirstWindow) }
            if (firstWindowLocked) {
                return@transaction claimSlot(
                    eventId, alignedStart, firstJitterMs, requestedTime, config.configId
                )
            }

            // Phase 2: Frontier-tracked find+lock
            val windowSize = config.windowSize

            val windowEnd = fetchWindowEnd(alignedStart) ?: initWindowEnd(alignedStart, windowSize)

            val foundInRange = with(windowSlotCounterRepository) {
                findAndLockFirstAvailableWindow(alignedStart.plus(windowSize), windowEnd, config.maxPerWindow)
            }
            if (foundInRange != null) {
                val jitterMs = computeFullWindowJitterMs(config.windowSizeMs)
                logger.info(
                    "Assigned slot for eventId={} in window={} (initial range)",
                    eventId, foundInRange
                )
                return@transaction claimSlot(
                    eventId, foundInRange, jitterMs, requestedTime, config.configId
                )
            }

            // Extension loop — extend from frontier
            var searchFrom = windowEnd
            for (chunk in 0 until maxChunksToSearch) {
                val chunkEnd = searchFrom.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))

                ensureChunkProvisioned(searchFrom, maxWindowsInChunk, windowSize)

                with(windowEndTrackerRepository) {
                    insertWindowEnd(alignedStart, chunkEnd)
                }

                val foundWindow = with(windowSlotCounterRepository) {
                    findAndLockFirstAvailableWindow(searchFrom, chunkEnd, config.maxPerWindow)
                }
                if (foundWindow != null) {
                    val jitterMs = computeFullWindowJitterMs(config.windowSizeMs)
                    logger.info(
                        "Assigned slot for eventId={} in window={} (extension chunk {})",
                        eventId, foundWindow, chunk
                    )
                    return@transaction claimSlot(
                        eventId, foundWindow, jitterMs, requestedTime, config.configId
                    )
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
    }

    // ---- Transaction helpers ----

    private fun Transaction.fetchWindowEnd(alignedStart: Instant): Instant? {
        return with(windowEndTrackerRepository) {
            fetchMaxWindowEnd(alignedStart)
        }
    }

    private fun Transaction.initWindowEnd(alignedStart: Instant, windowSize: Duration): Instant {
        val chunkStart = alignedStart.plus(windowSize)
        val windowEnd = chunkStart.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))
        ensureChunkProvisioned(chunkStart, maxWindowsInChunk, windowSize)
        with(windowEndTrackerRepository) {
            insertWindowEnd(alignedStart, windowEnd)
        }
        return windowEnd
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
