package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
import com.ratelimiter.repo.WindowEndTrackerRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V3 slot assignment — Kotlin/Exposed implementation.
 *
 * Uses a unified loop: iteration 0 reads the provisioning frontier (or provisions
 * the initial chunk if none exists), subsequent iterations extend by provisioning
 * new chunks. Each iteration searches with a CASE-based SQL query that applies
 * proportional capacity to the first window (alignedStart) and full capacity to
 * all others.
 *
 * DB work is split across multiple short-lived transactions to minimize
 * connection hold time and reduce pool contention under high TPS:
 * - Phase 0 (idempotency check) runs in its own transaction
 * - Frontier read + chunk provisioning runs in a single transaction
 * - Find+lock+claim runs in a focused transaction (holds row lock briefly)
 *
 * Uses raw SQL for lock queries (Exposed DSL doesn't support FOR UPDATE
 * SKIP LOCKED) and Exposed DSL for inserts and updates.
 */
@ApplicationScoped
class SlotAssignmentServiceV3(
    private val configRepository: RateLimitConfigRepository,
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    private val windowEndTrackerRepository: WindowEndTrackerRepository,
    @param:ConfigProperty(name = "rate-limiter.max-windows-in-chunk", defaultValue = "100")
    private val maxWindowsInChunk: Long,
    @param:ConfigProperty(name = "rate-limiter.max-chunks-to-search", defaultValue = "2")
    private val maxChunksToSearch: Int
) {

    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {

        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")

        val alignedStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val windowSize = config.windowSize

        // Phase 0: Pre-transaction idempotency check (own short-lived transaction).
        // Releases connection in ~1ms, avoids holding a connection through the main
        // loop for duplicate/retry requests.
        val existing = eventSlotRepository.fetchAssignedSlot(eventId)
        if (existing != null) {
            return existing
        }

        // Unified loop: iteration 0 reads frontier (or provisions initial chunk),
        // subsequent iterations extend by provisioning new chunks. Each iteration
        // searches the full range [alignedStart, chunkEnd) — re-scanning earlier
        // windows picks up rows that were SKIP LOCKED in previous passes.
        var provisionFrom = alignedStart
        for (iteration in 0 until maxChunksToSearch) {
            val chunkEnd = if (iteration == 0) {
                fetchOrProvisionChunk(alignedStart, windowSize)
            } else {
                val extensionEnd = provisionFrom.plus(windowSize.multipliedBy(maxWindowsInChunk))
                provisionChunk(provisionFrom, maxWindowsInChunk, windowSize, alignedStart, extensionEnd)
                extensionEnd
            }

            val found = findLockWindowAndClaimSlot(
                eventId, alignedStart, chunkEnd, requestedTime, config
            )
            if (found != null) return found

            provisionFrom = chunkEnd
        }

        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = maxChunksToSearch * maxWindowsInChunk,
            message = "Could not assign slot for event $eventId after searching " +
                    "$maxChunksToSearch chunks"
        )
    }

    // ---- Split-transaction helpers ----
    // Each helper runs its own short-lived transaction to minimize connection hold time.

    /**
     * Fetch the provisioning frontier for this alignedStart, or provision the initial
     * chunk if none exists. Returns the upper bound of the provisioned range.
     */
    private fun fetchOrProvisionChunk(alignedStart: Instant, windowSize: Duration): Instant {
        val existingFrontier = transaction {
            with(windowEndTrackerRepository) { fetchMaxWindowEnd(alignedStart) }
        }
        if (existingFrontier != null) return existingFrontier

        val chunkEnd = alignedStart.plus(windowSize.multipliedBy(maxWindowsInChunk))
        provisionChunk(alignedStart, maxWindowsInChunk, windowSize, alignedStart, chunkEnd)
        return chunkEnd
    }

    /**
     * Provision a chunk of windows and record the frontier.
     * Both operations are idempotent: ensureChunkProvisioned checks windowExists,
     * and insertWindowEnd catches duplicate keys.
     */
    private fun provisionChunk(
        from: Instant,
        windowCount: Long,
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
     * Uses a CASE expression to apply proportional capacity to alignedStart and full
     * capacity to all other windows. Returns null if no available window found in [alignedStart, to).
     */
    private fun findLockWindowAndClaimSlot(
        eventId: String,
        alignedStart: Instant,
        lastWindow: Instant,
        requestedTime: Instant,
        config: RateLimitConfig
    ): AssignedSlot? {
        val elapsedMs = Duration.between(alignedStart, requestedTime).toMillis()
        val maxFirstWindow = computeMaxSlotsInFirstWindow(config.maxPerWindow, elapsedMs, config.windowSizeMs)

        return transaction {
            val lockedWindow = with(windowSlotCounterRepository) {
                findAndLockFirstAvailableWindow(alignedStart, lastWindow, maxFirstWindow, config.maxPerWindow)
            } ?: return@transaction null

            val jitterMs = if (lockedWindow == alignedStart) {
                computeJitterMs(maxOf(elapsedMs, 0), config.windowSizeMs)
            } else {
                computeJitterMs(0, config.windowSizeMs)
            }
            claimSlot(eventId, lockedWindow, jitterMs, requestedTime, config.configId)
        }
    }

    /**
     * Batch-provision counter rows for a chunk of windows.
     * Guarded by an existence check on the last window — if it exists,
     * the entire chunk is already provisioned and we skip the batch.
     */
    private fun Transaction.ensureChunkProvisioned(
        from: Instant,
        windowCount: Long,
        windowSize: Duration
    ) {
        val lastWindow = from.plus(windowSize.multipliedBy((windowCount - 1)))
        val exists = with(windowSlotCounterRepository) { windowExists(lastWindow) }
        if (exists) return

        val windows = (0 until windowCount).map { i ->
            from.plus(windowSize.multipliedBy(i))
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

    private fun computeMaxSlotsInFirstWindow(maxPerWindow: Int, elapsedMs: Long, windowSizeMs: Long): Int {
        if (elapsedMs <= 0) return maxPerWindow
        val remainingMs = windowSizeMs - elapsedMs
        return Math.floorDiv(maxPerWindow.toLong() * remainingMs, windowSizeMs).toInt()
    }

    private fun computeJitterMs(lowerBoundMs: Long, upperBoundMs: Long): Long {
        return ThreadLocalRandom.current().nextLong(lowerBoundMs, upperBoundMs)
    }
}
