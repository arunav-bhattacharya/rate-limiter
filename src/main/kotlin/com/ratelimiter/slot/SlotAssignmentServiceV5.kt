package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.SkipPointerRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V5 slot assignment — optimistic INSERT with advisory counter table,
 * proximity-weighted random window selection, and DB-backed skip pointer.
 *
 * Three-phase allocation:
 *   Phase A: Normal — softMax within maxDuration, chunked into configurable
 *            batches (default 15 min) so proximity weighting stays tight.
 *   Phase B: Overflow — hardMax within maxDuration (fresh read from requestedTime)
 *   Phase C: Extension — softMax beyond maxDuration (fresh read per chunk)
 *
 * No row locks. No pre-provisioning. Counter rows created on demand via
 * UPDATE-first upsert with RETURNING INTO for hard-cap enforcement.
 *
 * DB calls per request (happy path): 3
 *   1. Skip pointer read       — PK lookup on RL_SKIP_PTR
 *   2. Advisory occupancy read — range scan on RL_WNDW_CT PK (one chunk)
 *   3. Slot insert + counter upsert — single transaction
 *
 * Idempotency is handled by the UNIQUE(EVENT_ID) constraint: duplicate inserts
 * are caught and the existing slot is re-read within the same transaction.
 */
@ApplicationScoped
class SlotAssignmentServiceV5(
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    private val skipPointerRepository: SkipPointerRepository,
    @param:ConfigProperty(name = "rate-limiter.window-size-seconds", defaultValue = "30")
    private val windowSizeSeconds: Long,
    @param:ConfigProperty(name = "rate-limiter.v5.soft-max-per-window", defaultValue = "870")
    private val softMaxPerWindow: Int,
    @param:ConfigProperty(name = "rate-limiter.v5.hard-max-per-window", defaultValue = "990")
    private val hardMaxPerWindow: Int,
    @param:ConfigProperty(name = "rate-limiter.v5.default-max-duration-hours", defaultValue = "8")
    private val defaultMaxDurationHours: Long,
    @param:ConfigProperty(name = "rate-limiter.v5.phase-a-chunk-seconds", defaultValue = "900")
    private val phaseAChunkSeconds: Long,
    @param:ConfigProperty(name = "rate-limiter.v5.extension-windows", defaultValue = "40")
    private val extensionWindows: Int,
    @param:ConfigProperty(name = "rate-limiter.v5.max-extensions-beyond", defaultValue = "5")
    private val maxExtensionsBeyond: Int,
    @param:ConfigProperty(name = "rate-limiter.v5.max-hard-cap-retries", defaultValue = "3")
    private val maxHardCapRetries: Int,
) {
    companion object {
        const val STATIC_CONFIG_ID = "STATIC"
    }

    private val windowSize: Duration = Duration.ofSeconds(windowSizeSeconds)
    private val windowSizeMs: Long = windowSizeSeconds * 1000
    private val phaseAChunkDuration: Duration = Duration.ofSeconds(phaseAChunkSeconds)
    private val extensionDuration: Duration = windowSize.multipliedBy(extensionWindows.toLong())
    val defaultMaxDuration: Duration = Duration.ofHours(defaultMaxDurationHours)

    fun assignSlot(eventId: String, requestedTime: Instant, maxDuration: Duration = defaultMaxDuration): AssignedSlot {
        // Step 1: Read DB skip pointer
        val skipTo = skipPointerRepository.fetchSkipTo(requestedTime) ?: requestedTime
        val maxDurationEnd = requestedTime.plus(maxDuration)
        val phaseAStart = maxOf(skipTo, requestedTime)

        // ---- PHASE A: Normal allocation within maxDuration (softMax, chunked) ----
        // Advances skip pointer per-chunk as each chunk is exhausted.
        val phaseAResult = phaseA(eventId, requestedTime, phaseAStart, maxDurationEnd)
        if (phaseAResult != null) return phaseAResult

        // ---- PHASE B: Overflow within maxDuration (hardMax, fresh read) ----
        val phaseBResult = phaseB(eventId, requestedTime, maxDurationEnd)
        if (phaseBResult != null) return phaseBResult

        // ---- PHASE C: Extension beyond maxDuration (softMax, fresh read per chunk) ----
        val phaseCResult = phaseC(eventId, requestedTime, maxDurationEnd)
        if (phaseCResult != null) return phaseCResult

        val phaseCEnd = maxDurationEnd.plus(extensionDuration.multipliedBy(maxExtensionsBeyond.toLong()))
        val totalWindowsSearched = Duration.between(requestedTime, phaseCEnd).toSeconds() / windowSizeSeconds
        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = totalWindowsSearched,
            message = "No available window for event $eventId within $totalWindowsSearched windows"
        )
    }

    // ---- Phase implementations ----

    /**
     * Phase A: scan forward from skipTo one chunk at a time (default 15 min).
     * Each chunk reads its own occupancy and picks within that narrow window set.
     * Advances skip pointer per-chunk as each is exhausted, so other pods
     * and subsequent requests skip already-exhausted chunks immediately.
     */
    private fun phaseA(
        eventId: String,
        requestedTime: Instant,
        phaseAStart: Instant,
        maxDurationEnd: Instant
    ): AssignedSlot? {
        var chunkStart = phaseAStart
        while (chunkStart < maxDurationEnd) {
            val chunkEnd = minOf(chunkStart.plus(phaseAChunkDuration), maxDurationEnd)

            val occupancy = windowSlotCounterRepository.readOccupancy(chunkStart, chunkEnd)
            val windows = generateWindowsInRange(chunkStart, chunkEnd)
            val picked = pickProximityWeightedRandom(windows, occupancy, softMaxPerWindow)

            if (picked != null) {
                val result = tryClaimWithHardCapRetry(
                    eventId, requestedTime, picked, chunkStart, chunkEnd,
                    softMaxPerWindow, AllocationStatus.NORMAL
                )
                if (result != null) return result
            }

            // Chunk exhausted at softMax — advance skip pointer so other pods skip it
            skipPointerRepository.advanceSkipTo(requestedTime, chunkEnd)
            chunkStart = chunkEnd
        }
        return null
    }

    private fun phaseB(
        eventId: String,
        requestedTime: Instant,
        maxDurationEnd: Instant
    ): AssignedSlot? {
        // Fresh occupancy read from requestedTime (not skipTo) — windows between
        // softMax and hardMax may exist before the skip pointer.
        val freshOccupancy = windowSlotCounterRepository.readOccupancy(requestedTime, maxDurationEnd)
        val windows = generateWindowsInRange(requestedTime, maxDurationEnd)
        val picked = pickProximityWeightedRandom(windows, freshOccupancy, hardMaxPerWindow)
            ?: return null

        val result = tryClaimWithHardCapRetry(
            eventId, requestedTime, picked, requestedTime, maxDurationEnd,
            hardMaxPerWindow, AllocationStatus.SOFT_MAX_EXCEEDED
        )
        return result
    }

    /**
     * Phase C: extend beyond maxDuration in chunks, each with its own fresh
     * occupancy read. Only reached when Phases A and B are exhausted.
     */
    private fun phaseC(
        eventId: String,
        requestedTime: Instant,
        maxDurationEnd: Instant
    ): AssignedSlot? {
        var extStart = maxDurationEnd
        for (iteration in 0 until maxExtensionsBeyond) {
            val extEnd = extStart.plus(extensionDuration)

            val occupancy = windowSlotCounterRepository.readOccupancy(extStart, extEnd)
            val windows = generateWindowsInRange(extStart, extEnd)
            val picked = pickProximityWeightedRandom(windows, occupancy, softMaxPerWindow)

            if (picked != null) {
                val result = tryClaimWithHardCapRetry(
                    eventId, requestedTime, picked, extStart, extEnd,
                    softMaxPerWindow, AllocationStatus.MAX_DURATION_EXCEEDED
                )
                if (result != null) return result
            }

            // Advance skip pointer as we exhaust extension ranges
            skipPointerRepository.advanceSkipTo(requestedTime, extEnd)
            extStart = extEnd
        }
        return null
    }

    // ---- Claim + hard-cap retry ----

    /**
     * Attempts to claim the picked window. If hard cap is hit (transaction rolled back),
     * re-reads occupancy and retries with a different window up to [maxHardCapRetries] times.
     */
    private fun tryClaimWithHardCapRetry(
        eventId: String,
        requestedTime: Instant,
        initialPick: Instant,
        rangeStart: Instant,
        rangeEnd: Instant,
        threshold: Int,
        status: AllocationStatus
    ): AssignedSlot? {
        // First attempt with initial pick
        val jitterMs = ThreadLocalRandom.current().nextLong(0, windowSizeMs)
        val scheduledTime = initialPick.plusMillis(jitterMs)
        val result = claimSlotAndUpdateCounter(eventId, requestedTime, initialPick, scheduledTime, status)
        if (result != null) return result

        // Hard cap hit — retry with fresh occupancy
        for (retry in 1..maxHardCapRetries) {
            val freshOccupancy = windowSlotCounterRepository.readOccupancy(rangeStart, rangeEnd)
            val windows = generateWindowsInRange(rangeStart, rangeEnd)
            val retryPick = pickProximityWeightedRandom(windows, freshOccupancy, threshold)
                ?: return null // All windows in range are full

            val retryJitter = ThreadLocalRandom.current().nextLong(0, windowSizeMs)
            val retryScheduled = retryPick.plusMillis(retryJitter)
            val retryResult = claimSlotAndUpdateCounter(eventId, requestedTime, retryPick, retryScheduled, status)
            if (retryResult != null) return retryResult
        }
        return null
    }

    /**
     * Insert event slot + upsert counter in a single short-lived transaction.
     * Returns null if hard cap is breached (transaction rolled back).
     * Handles duplicate EVENT_ID via catch (idempotency: re-reads existing slot).
     */
    private fun claimSlotAndUpdateCounter(
        eventId: String,
        requestedTime: Instant,
        windowStart: Instant,
        scheduledTime: Instant,
        status: AllocationStatus
    ): AssignedSlot? {
        return transaction {
            val inserted = with(eventSlotRepository) {
                insertEventSlot(eventId, requestedTime, windowStart, scheduledTime, STATIC_CONFIG_ID)
            }

            if (!inserted) {
                // Duplicate eventId — idempotency
                return@transaction with(eventSlotRepository) { queryAssignedSlot(eventId) }
                    ?: error("Failed to re-read slot for eventId=$eventId after duplicate key")
            }

            val newCount = with(windowSlotCounterRepository) {
                upsertCounterReturningCount(windowStart)
            }

            if (newCount > hardMaxPerWindow) {
                // Hard cap breached — rollback everything (slot INSERT + counter increment)
                rollback()
                return@transaction null
            }

            val delay = Duration.between(requestedTime, scheduledTime).let { d ->
                if (d.isNegative) Duration.ZERO else d
            }
            AssignedSlot(
                eventId = eventId,
                scheduledTime = scheduledTime,
                delay = delay,
                allocationStatus = status
            )
        }
    }

    private fun generateWindowsInRange(from: Instant, to: Instant): List<Instant> {
        return generateSequence(from) { it + windowSize }
            .takeWhile { it < to }
            .toList()
    }

    /**
     * Proximity-weighted random selection: windows closer to the start of the range
     * AND with more remaining capacity are proportionally more likely to be selected.
     *
     * weight(W) = capacityWeight * proximityWeight
     *   capacityWeight = max(0, threshold - occupancy(W))
     *   proximityWeight = rangeSize - index  (linear decay)
     *
     * Windows at or above the threshold are excluded (capacityWeight = 0).
     */
    private fun pickProximityWeightedRandom(
        windows: List<Instant>,
        occupancy: Map<Instant, Int>,
        threshold: Int
    ): Instant? {
        val rangeSize = windows.size
        val candidates = windows.mapIndexed { index, window ->
            val capacityWeight = maxOf(0, threshold - (occupancy[window] ?: 0))
            val proximityWeight = rangeSize - index
            window to (capacityWeight.toLong() * proximityWeight)
        }.filter { it.second > 0 }

        if (candidates.isEmpty()) return null

        val totalWeight = candidates.sumOf { it.second }
        var roll = ThreadLocalRandom.current().nextLong(totalWeight)
        for ((window, weight) in candidates) {
            roll -= weight
            if (roll < 0) return window
        }
        return candidates.last().first
    }
}
