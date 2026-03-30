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
import kotlin.math.floor

/**
 * V5 slot assignment — optimistic INSERT with advisory counter table,
 * proximity-weighted random window selection, and DB-backed skip pointer.
 *
 * Three-phase allocation:
 *   Phase 1: Normal — softMax within maxDuration, chunked into configurable
 *            batches (default 15 min) so proximity weighting stays tight.
 *   Phase 2: Overflow — maxSlotsPerWindow within maxDuration (fresh read from requestedTime)
 *   Phase 3: Extension — softMax beyond maxDuration (fresh read per chunk)
 *
 * Capacity:
 *   softMax          = floor(maxSlots * softMaxPercent / 100)  — Phase 1 operating limit
 *   maxSlotsPerWindow = configured ceiling                     — no window ever exceeds this
 *
 * No row locks. No pre-provisioning. No rollback on capacity overshoot.
 * Counter rows created on demand via UPDATE-first upsert.
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
    private val windowPicker: WindowPicker,
    @param:ConfigProperty(name = "rate-limiter.v5.window-size", defaultValue = "30s")
    private val windowSize: Duration,
    @param:ConfigProperty(name = "rate-limiter.v5.max-slots-per-window", defaultValue = "900")
    private val maxSlotsPerWindow: Int,
    @param:ConfigProperty(name = "rate-limiter.v5.soft-max-percent", defaultValue = "90")
    private val softMaxPercent: Int,
    @param:ConfigProperty(name = "rate-limiter.v5.default-max-duration", defaultValue = "8h")
    val defaultMaxDuration: Duration,
    @param:ConfigProperty(name = "rate-limiter.v5.window-chunk-duration", defaultValue = "15m")
    private val windowChunkDuration: Duration,
    @param:ConfigProperty(name = "rate-limiter.v5.extension-windows", defaultValue = "40")
    private val extensionWindows: Int,
    @param:ConfigProperty(name = "rate-limiter.v5.max-extensions-beyond", defaultValue = "5")
    private val maxExtensionsBeyond: Int,
) {
    companion object {
        const val STATIC_CONFIG_ID = "STATIC"
    }

    private val windowSizeMs: Long = windowSize.toMillis()
    private val windowSizeSeconds: Long = windowSize.toSeconds()
    private val extensionDuration: Duration = windowSize.multipliedBy(extensionWindows.toLong())

    /** Phase 1 operating limit: floor(maxSlots * softMaxPercent / 100) */
    private val softMax: Int = floor(maxSlotsPerWindow * softMaxPercent / 100.0).toInt()

    fun assignSlot(eventId: String, requestedTime: Instant, maxDuration: Duration = defaultMaxDuration): AssignedSlot {
        // Step 1: Read DB skip pointer
        val skipTo = skipPointerRepository.fetchSkipTo(requestedTime) ?: requestedTime
        val maxDurationEnd = requestedTime.plus(maxDuration)
        val phase1Start = maxOf(skipTo, requestedTime)

        // ---- PHASE 1: Normal allocation within maxDuration (softMax, chunked) ----
        // Advances skip pointer per-chunk as each chunk is exhausted.
        val phase1Result = phase1(eventId, requestedTime, phase1Start, maxDurationEnd)
        if (phase1Result != null) return phase1Result

        // ---- PHASE 2: Overflow within maxDuration (maxSlots, fresh read) ----
        val phase2Result = phase2(eventId, requestedTime, maxDurationEnd)
        if (phase2Result != null) return phase2Result

        // ---- PHASE 3: Extension beyond maxDuration (softMax, fresh read per chunk) ----
        val phase3Result = phase3(eventId, requestedTime, maxDurationEnd)
        if (phase3Result != null) return phase3Result

        val phase3End = maxDurationEnd.plus(extensionDuration.multipliedBy(maxExtensionsBeyond.toLong()))
        val totalWindowsSearched = Duration.between(requestedTime, phase3End).toSeconds() / windowSizeSeconds
        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = totalWindowsSearched,
            message = "No available window for event $eventId within $totalWindowsSearched windows"
        )
    }

    // ---- Phase implementations ----

    /**
     * Phase 1: scan forward from skipTo one chunk at a time (default 15 min).
     * Each chunk reads its own occupancy and picks within that narrow window set.
     * Advances skip pointer per-chunk as each is exhausted, so other pods
     * and subsequent requests skip already-exhausted chunks immediately.
     */
    private fun phase1(
        eventId: String,
        requestedTime: Instant,
        phase1Start: Instant,
        maxDurationEnd: Instant
    ): AssignedSlot? {
        var chunkStart = phase1Start
        while (chunkStart < maxDurationEnd) {
            val chunkEnd = minOf(chunkStart.plus(windowChunkDuration), maxDurationEnd)

            val occupancy = windowSlotCounterRepository.readOccupancy(chunkStart, chunkEnd)
            val windows = generateWindowsInRange(chunkStart, chunkEnd)
            val picked = windowPicker.pickProximityWeightedRandom(windows, occupancy, softMax)

            if (picked != null) {
                return claimSlot(eventId, requestedTime, picked, AllocationStatus.NORMAL)
            }

            // Chunk exhausted at softMax — advance skip pointer so other pods skip it
            skipPointerRepository.advanceSkipTo(requestedTime, chunkEnd)
            chunkStart = chunkEnd
        }
        return null
    }

    /**
     * Phase 2: overflow within maxDuration using maxSlotsPerWindow threshold.
     * Fresh occupancy read from requestedTime (not skipTo) — windows between
     * softMax and maxSlotsPerWindow may exist before the skip pointer.
     */
    private fun phase2(
        eventId: String,
        requestedTime: Instant,
        maxDurationEnd: Instant
    ): AssignedSlot? {
        val freshOccupancy = windowSlotCounterRepository.readOccupancy(requestedTime, maxDurationEnd)
        val windows = generateWindowsInRange(requestedTime, maxDurationEnd)
        val picked = windowPicker.pickProximityWeightedRandom(windows, freshOccupancy, maxSlotsPerWindow)
            ?: return null

        return claimSlot(eventId, requestedTime, picked, AllocationStatus.SOFT_MAX_EXCEEDED)
    }

    /**
     * Phase 3: extend beyond maxDuration in chunks, each with its own fresh
     * occupancy read. Only reached when Phases 1 and 2 are exhausted.
     */
    private fun phase3(
        eventId: String,
        requestedTime: Instant,
        maxDurationEnd: Instant
    ): AssignedSlot? {
        var extStart = maxDurationEnd
        for (iteration in 0 until maxExtensionsBeyond) {
            val extEnd = extStart.plus(extensionDuration)

            val occupancy = windowSlotCounterRepository.readOccupancy(extStart, extEnd)
            val windows = generateWindowsInRange(extStart, extEnd)
            val picked = windowPicker.pickProximityWeightedRandom(windows, occupancy, softMax)

            if (picked != null) {
                return claimSlot(eventId, requestedTime, picked, AllocationStatus.MAX_DURATION_EXCEEDED)
            }

            // Advance skip pointer as we exhaust extension ranges
            skipPointerRepository.advanceSkipTo(requestedTime, extEnd)
            extStart = extEnd
        }
        return null
    }

    // ---- Claim ----

    /**
     * Claims a slot in the picked window. Always succeeds (no rollback on capacity overshoot).
     * Downstream tolerance absorbs the marginal overshoot from racing.
     */
    private fun claimSlot(
        eventId: String,
        requestedTime: Instant,
        pickedWindow: Instant,
        status: AllocationStatus
    ): AssignedSlot {
        val jitterMs = ThreadLocalRandom.current().nextLong(0, windowSizeMs)
        val scheduledTime = pickedWindow.plusMillis(jitterMs)
        return claimSlotAndUpdateCounter(eventId, requestedTime, pickedWindow, scheduledTime, status)
    }

    /**
     * Insert event slot + upsert counter in a single short-lived transaction.
     * Handles duplicate EVENT_ID via catch (idempotency: re-reads existing slot).
     * No rollback on capacity overshoot — downstream tolerance absorbs marginal racing.
     */
    private fun claimSlotAndUpdateCounter(
        eventId: String,
        requestedTime: Instant,
        windowStart: Instant,
        scheduledTime: Instant,
        status: AllocationStatus
    ): AssignedSlot {
        return transaction {
            val inserted = with(eventSlotRepository) {
                insertEventSlot(eventId, requestedTime, windowStart, scheduledTime, STATIC_CONFIG_ID)
            }

            if (!inserted) {
                // Duplicate eventId — idempotency
                return@transaction with(eventSlotRepository) { queryAssignedSlot(eventId) }
                    ?: error("Failed to re-read slot for eventId=$eventId after duplicate key")
            }

            with(windowSlotCounterRepository) {
                upsertCounter(windowStart)
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

}
