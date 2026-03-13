package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
import com.ratelimiter.repo.WindowEndTrackerRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

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
 * Background pre-provisioning ensures request threads never pay provisioning cost
 * after cold start. Pre-provisions both the next chunk for the current alignedStart
 * and the first window + initial chunk for the next alignedStart boundary.
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
    private val maxChunksToSearch: Int,
    @param:ConfigProperty(name = "rate-limiter.pre-provision-enabled", defaultValue = "true")
    private val preProvisionEnabled: Boolean = true
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentServiceV3::class.java)
    private val firstWindowFull = ConcurrentHashMap<Instant, Boolean>()
    private val preProvisionSubmitted = ConcurrentHashMap.newKeySet<Instant>()
    private val preProvisionExecutor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    @PreDestroy
    fun shutdown() {
        preProvisionExecutor.shutdown()
        preProvisionExecutor.awaitTermination(5, TimeUnit.SECONDS)
    }

    fun evictFirstWindowCache() {
        firstWindowFull.clear()
        preProvisionSubmitted.clear()
    }

    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {

        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")

        val alignedStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val elapsedMs = Duration.between(alignedStart, requestedTime).toMillis()
        val maxFirstWindow = computeEffectiveMax(config.maxPerWindow, elapsedMs, config.windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, config.windowSizeMs)

        // Phase 0: Pre-transaction idempotency check (own short-lived transaction).
        // Releases connection in ~1ms, avoids holding a connection through Phases 1-2
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
            if (firstWindowSlot != null) {
                maybePreProvisionNextAlignedStart(alignedStart, config.windowSize)
                return firstWindowSlot
            }
        }

        // Phase 2: Frontier-tracked find+lock with extension
        // Iteration 0 searches the initial provisioned range; iterations 1..maxChunksToSearch extend beyond.
        val windowSize = config.windowSize
        var searchFrom = alignedStart.plus(windowSize)

        for (iteration in 0..<maxChunksToSearch) {
            val searchTo = if (iteration == 0) {
                fetchOrProvisionInitialRange(alignedStart, windowSize)
            } else {
                val chunkEnd = searchFrom.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))
                provisionChunk(searchFrom, maxWindowsInChunk, windowSize, alignedStart, chunkEnd)
                chunkEnd
            }

            val found = findLockAndClaim(
                eventId, searchFrom, searchTo,
                config.maxPerWindow, windowSize, requestedTime, config.configId
            )
            if (found != null) {
                maybePreProvisionNextChunk(searchTo, windowSize, alignedStart)
                maybePreProvisionNextAlignedStart(alignedStart, windowSize)
                return found
            }

            searchFrom = searchTo
        }

        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = maxWindowsInChunk + ((maxChunksToSearch - 1) * maxWindowsInChunk),
            message = "Could not assign slot for event $eventId after searching " +
                    "initial range + ${maxChunksToSearch - 1} extension chunks"
        )
    }

    // ---- Split-transaction helpers ----
    // Each helper runs its own short-lived transaction to minimize connection hold time.

    /**
     * Fetch the provisioning frontier for this alignedStart, or provision the initial range
     * if none exists.
     */
    private fun fetchOrProvisionInitialRange(alignedStart: Instant, windowSize: Duration): Instant {
        val existingFrontier = transaction {
            with(windowEndTrackerRepository) { fetchMaxWindowEnd(alignedStart) }
        }
        if (existingFrontier != null) return existingFrontier

        val chunkStart = alignedStart.plus(windowSize)
        val windowEnd = chunkStart.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))
        transaction {
            ensureChunkProvisioned(chunkStart, maxWindowsInChunk, windowSize)
            with(windowEndTrackerRepository) { insertWindowEnd(alignedStart, windowEnd) }
        }
        return windowEnd
    }

    /**
     * Provision a chunk of windows and record the frontier.
     * Frontier insert is idempotent.
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
            with(windowEndTrackerRepository) {
                insertWindowEnd(alignedStart, chunkEnd)
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
                findAndLockFirstAvailableWindow(from, to, maxSlots)
            } ?: return@transaction null

            val jitterMs = computeFullWindowJitterMs(windowSize.toMillis())
            claimSlot(eventId, found, jitterMs, requestedTime, configId)
        }
    }

    // ---- Background pre-provisioning ----

    /**
     * Pre-provision the next chunk beyond the current frontier for this alignedStart.
     * Triggered after a successful Phase 2 slot claim. Each trigger provisions exactly
     * one chunk ahead — no chain reaction. Growth is bounded by actual consumption.
     */
    private fun maybePreProvisionNextChunk(currentFrontier: Instant, windowSize: Duration, alignedStart: Instant) {
        if (!preProvisionEnabled) return
        if (!preProvisionSubmitted.add(currentFrontier)) return  // atomic CAS gate

        preProvisionExecutor.execute {
            try {
                val chunkEnd = currentFrontier.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))
                transaction {
                    ensureChunkProvisioned(currentFrontier, maxWindowsInChunk, windowSize)
                    with(windowEndTrackerRepository) { insertWindowEnd(alignedStart, chunkEnd) }
                }
            } catch (e: Exception) {
                preProvisionSubmitted.remove(currentFrontier)
                logger.warn("Pre-provision next chunk failed at {}: {}", currentFrontier, e.message)
            }
        }
    }

    /**
     * Pre-provision the first window and initial chunk for the next alignedStart boundary.
     * Triggered after any successful slot claim (Phase 1 or Phase 2). Ensures the next
     * alignedStart boundary has everything ready so requests never pay provisioning cost.
     */
    private fun maybePreProvisionNextAlignedStart(alignedStart: Instant, windowSize: Duration) {
        if (!preProvisionEnabled) return

        val nextAlignedStart = alignedStart.plus(windowSize)
        val nextChunkStart = nextAlignedStart.plus(windowSize)
        if (!preProvisionSubmitted.add(nextChunkStart)) return  // atomic CAS gate

        preProvisionExecutor.execute {
            try {
                // First window for next alignedStart
                transaction {
                    with(windowSlotCounterRepository) { ensureWindowExists(nextAlignedStart) }
                }
                // Initial chunk + frontier for next alignedStart
                val chunkEnd = nextChunkStart.plus(windowSize.multipliedBy(maxWindowsInChunk.toLong()))
                transaction {
                    ensureChunkProvisioned(nextChunkStart, maxWindowsInChunk, windowSize)
                    with(windowEndTrackerRepository) { insertWindowEnd(nextAlignedStart, chunkEnd) }
                }
            } catch (e: Exception) {
                preProvisionSubmitted.remove(nextChunkStart)
                logger.warn("Pre-provision next alignedStart failed at {}: {}", nextAlignedStart, e.message)
            }
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
        with(windowSlotCounterRepository) { batchInsertWindows(windows) }
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
