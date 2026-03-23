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
 * Assumes requestedTime is always aligned to a window boundary.
 *
 * Uses a unified loop: iteration 0 reads the provisioning frontier (or provisions
 * the initial chunk if none exists), subsequent iterations extend by provisioning
 * new chunks. Each iteration searches for available windows using a single sargable
 * range query with full capacity — no CASE expression, no proportional capacity.
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

        val windowSize = config.windowSize

        // Phase 0: Pre-transaction idempotency check (own short-lived transaction).
        val existing = eventSlotRepository.fetchAssignedSlot(eventId)
        if (existing != null) {
            return existing
        }

        // Unified loop: iteration 0 reads frontier (or provisions initial chunk),
        // subsequent iterations extend by provisioning new chunks. Iteration 0
        // scans from requestedTime; iteration 1+ scans only the newly provisioned
        // chunk to avoid re-traversing the already-exhausted range.
        var provisionFrom = requestedTime
        for (iteration in 0 until maxChunksToSearch) {
            val chunkEnd = if (iteration == 0) {
                fetchOrProvisionChunk(requestedTime, windowSize)
            } else {
                val extensionEnd = provisionFrom.plus(windowSize.multipliedBy(maxWindowsInChunk))
                provisionChunk(provisionFrom, maxWindowsInChunk, windowSize, requestedTime, extensionEnd)
                extensionEnd
            }

            val found = findLockWindowAndClaimSlot(eventId, provisionFrom, chunkEnd, requestedTime, config)
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

    /**
     * Fetch the provisioning frontier for this requestedTime, or provision the initial
     * chunk if none exists. Returns the upper bound of the provisioned range.
     *
     * Fast path: checks JVM cache without acquiring a connection (~99% of calls).
     * Slow path: cache miss → DB query in a short-lived transaction.
     */
    private fun fetchOrProvisionChunk(requestedTime: Instant, windowSize: Duration): Instant {
        windowEndTrackerRepository.fetchMaxWindowEndCached(requestedTime)?.let { return it }

        val dbFrontier = transaction {
            with(windowEndTrackerRepository) { fetchMaxWindowEndFromDb(requestedTime) }
        }
        if (dbFrontier != null) return dbFrontier

        val chunkEnd = requestedTime.plus(windowSize.multipliedBy(maxWindowsInChunk))
        provisionChunk(requestedTime, maxWindowsInChunk, windowSize, requestedTime, chunkEnd)
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
        requestedTime: Instant,
        chunkEnd: Instant
    ) {
        transaction {
            ensureChunkProvisioned(from, windowCount, windowSize)
            with(windowEndTrackerRepository) {
                insertWindowEnd(requestedTime, chunkEnd)
            }
        }
    }

    /**
     * Find an available window, lock it, and claim the slot — all in one short transaction.
     * Single sargable range query with full capacity (SLOT_CT < maxPerWindow).
     */
    private fun findLockWindowAndClaimSlot(
        eventId: String,
        scanStart: Instant,
        lastWindow: Instant,
        requestedTime: Instant,
        config: RateLimitConfig
    ): AssignedSlot? {
        return transaction {
            val lockedWindow = with(windowSlotCounterRepository) {
                lockFirstAvailableInRange(scanStart, lastWindow, config.maxPerWindow)
            } ?: return@transaction null

            val jitterMs = ThreadLocalRandom.current().nextLong(0, config.windowSizeMs)
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
}
