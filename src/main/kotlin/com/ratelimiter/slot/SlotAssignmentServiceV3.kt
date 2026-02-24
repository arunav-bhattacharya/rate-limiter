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
    private val firstWindowFull = ConcurrentHashMap<Instant, Boolean>()

    fun evictFirstWindowCache() = firstWindowFull.clear()

    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        val totalStart = System.nanoTime()

        var t0 = System.nanoTime()
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")
        logger.debug("eventId={} | loadConfig took {}ms", eventId, nanosToMs(System.nanoTime() - t0))

        val alignedStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val elapsedMs = Duration.between(alignedStart, requestedTime).toMillis()
        val maxFirstWindow = computeEffectiveMax(config.maxPerWindow, elapsedMs, config.windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, config.windowSizeMs)

        return transaction {
            // Phase 0: Idempotency check
            t0 = System.nanoTime()
            val existing = with(eventSlotRepository) { queryAssignedSlot(eventId) }
            logger.debug("eventId={} | idempotencyCheck took {}ms", eventId, nanosToMs(System.nanoTime() - t0))
            if (existing != null) {
                logger.info("eventId={} | totalTime={}ms (idempotent hit)", eventId, nanosToMs(System.nanoTime() - totalStart))
                return@transaction existing
            }

            // Phase 1: First window (proportional capacity)
            if (!firstWindowFull.containsKey(alignedStart)) {
                t0 = System.nanoTime()
                with(windowSlotCounterRepository) { ensureWindowExists(alignedStart) }
                logger.debug("eventId={} | ensureFirstWindowExists took {}ms", eventId, nanosToMs(System.nanoTime() - t0))

                t0 = System.nanoTime()
                val lockResult = with(windowSlotCounterRepository) { tryLockFirstWindow(alignedStart, maxFirstWindow) }
                logger.debug("eventId={} | tryLockFirstWindow took {}ms (result={})", eventId, nanosToMs(System.nanoTime() - t0), lockResult)

                when (lockResult) {
                    true -> {
                        t0 = System.nanoTime()
                        val slot = claimSlot(eventId, alignedStart, firstJitterMs, requestedTime, config.configId)
                        logger.debug("eventId={} | claimSlot (firstWindow) took {}ms", eventId, nanosToMs(System.nanoTime() - t0))
                        logger.info("eventId={} | totalTime={}ms (firstWindow)", eventId, nanosToMs(System.nanoTime() - totalStart))
                        return@transaction slot
                    }
                    false -> firstWindowFull[alignedStart] = true
                    null -> {} // SKIP LOCKED — contended, try again next request
                }
            }

            // Phase 2: Frontier-tracked find+lock
            val windowSize = config.windowSize

            t0 = System.nanoTime()
            val windowEnd = fetchWindowEnd(alignedStart) ?: initWindowEnd(alignedStart, windowSize)
            logger.debug("eventId={} | fetchOrInitWindowEnd took {}ms", eventId, nanosToMs(System.nanoTime() - t0))

            t0 = System.nanoTime()
            val foundInRange = with(windowSlotCounterRepository) {
                findAndLockFirstAvailableWindow(alignedStart.plus(windowSize), windowEnd, config.maxPerWindow)
            }
            logger.debug("eventId={} | findAndLock (initialRange) took {}ms", eventId, nanosToMs(System.nanoTime() - t0))

            if (foundInRange != null) {
                val jitterMs = computeFullWindowJitterMs(config.windowSizeMs)
                t0 = System.nanoTime()
                val slot = claimSlot(eventId, foundInRange, jitterMs, requestedTime, config.configId)
                logger.debug("eventId={} | claimSlot (initialRange) took {}ms", eventId, nanosToMs(System.nanoTime() - t0))
                logger.info("eventId={} | totalTime={}ms (initialRange, window={})", eventId, nanosToMs(System.nanoTime() - totalStart), foundInRange)
                return@transaction slot
            }

            // Extension loop — extend from frontier
            var searchFrom = windowEnd
            for (chunk in 0 until maxChunksToSearch) {
                val chunkEnd = searchFrom.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))

                t0 = System.nanoTime()
                ensureChunkProvisioned(searchFrom, maxWindowsInChunk, windowSize)
                logger.debug("eventId={} | ensureChunkProvisioned (chunk={}) took {}ms", eventId, chunk, nanosToMs(System.nanoTime() - t0))

                t0 = System.nanoTime()
                with(windowEndTrackerRepository) {
                    insertWindowEnd(alignedStart, chunkEnd)
                }
                logger.debug("eventId={} | insertWindowEnd (chunk={}) took {}ms", eventId, chunk, nanosToMs(System.nanoTime() - t0))

                t0 = System.nanoTime()
                val foundWindow = with(windowSlotCounterRepository) {
                    findAndLockFirstAvailableWindow(searchFrom, chunkEnd, config.maxPerWindow)
                }
                logger.debug("eventId={} | findAndLock (chunk={}) took {}ms", eventId, chunk, nanosToMs(System.nanoTime() - t0))

                if (foundWindow != null) {
                    val jitterMs = computeFullWindowJitterMs(config.windowSizeMs)
                    t0 = System.nanoTime()
                    val slot = claimSlot(eventId, foundWindow, jitterMs, requestedTime, config.configId)
                    logger.debug("eventId={} | claimSlot (chunk={}) took {}ms", eventId, chunk, nanosToMs(System.nanoTime() - t0))
                    logger.info("eventId={} | totalTime={}ms (extensionChunk={}, window={})", eventId, nanosToMs(System.nanoTime() - totalStart), chunk, foundWindow)
                    return@transaction slot
                }

                searchFrom = chunkEnd
            }

            logger.warn("eventId={} | totalTime={}ms (exhausted)", eventId, nanosToMs(System.nanoTime() - totalStart))
            throw SlotAssignmentException(
                eventId = eventId,
                windowsSearched = maxWindowsInChunk + (maxChunksToSearch * maxWindowsInChunk),
                message = "Could not assign slot for event $eventId after searching " +
                        "initial range + $maxChunksToSearch extension chunks"
            )
        }
    }

    private fun nanosToMs(nanos: Long): String = "%.3f".format(nanos / 1_000_000.0)

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
