package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V7 slot assignment — simplified single-phase with occupancy-weighted selection.
 *
 * Simplifications over V6:
 *   - Single phase: no softMax overflow, no extension beyond maxDuration
 *   - No skip pointer: always starts from requestedTime
 *   - No soft guard: trusts background-maintained STATUS column
 *   - No in-memory window generation: queries pre-provisioned windows from DB
 *   - Occupancy-only weighting: no proximity factor
 *
 * Hot path (2 DB calls):
 *   1. Fetch N available windows — index scan on (WNDW_STATUS, WNDW_STRT_TS)
 *   2. Slot INSERT — single row into RL_EVENT_SLOT_DTL
 *
 * Window lifecycle:
 *   - Pre-provisioned 60 days ahead by [WindowPreProvisioningScheduler]
 *   - Counter + status maintained by [WindowCounterRefreshJob]
 *   - STATUS transitions: AVAILABLE → FULL (when SLOT_CT >= maxSlotsPerWindow)
 *
 * maxDuration is required per-request (no default).
 */
@ApplicationScoped
class SlotAssignmentServiceV7(
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    private val windowPicker: WindowPicker,
    @param:ConfigProperty(name = "rate-limiter.v7.window-size", defaultValue = "30s")
    private val windowSize: Duration,
    @param:ConfigProperty(name = "rate-limiter.v7.max-slots-per-window", defaultValue = "900")
    private val maxSlotsPerWindow: Int,
    @param:ConfigProperty(name = "rate-limiter.v7.candidate-window-count", defaultValue = "30")
    private val candidateWindowCount: Int,
) {
    companion object {
        const val STATIC_CONFIG_ID = "STATIC"
    }

    private val windowSizeMs: Long = windowSize.toMillis()
    private val windowSizeSeconds: Long = windowSize.toSeconds()

    /**
     * Assigns a slot for [eventId] within [requestedTime, requestedTime + maxDuration).
     *
     * @param eventId unique event identifier (idempotency key)
     * @param requestedTime desired execution time
     * @param maxDuration maximum duration from requestedTime for slot placement (required)
     * @throws SlotAssignmentException if no available window exists in the range
     */
    fun assignSlot(eventId: String, requestedTime: Instant, maxDuration: Duration): AssignedSlot {
        val maxDurationEnd = requestedTime.plus(maxDuration)

        // Fetch candidate windows with capacity
        val candidates = windowSlotCounterRepository.fetchAvailableWindows(
            requestedTime, maxDurationEnd, candidateWindowCount
        )

        if (candidates.isEmpty()) {
            val totalWindows = Duration.between(requestedTime, maxDurationEnd).toSeconds() / windowSizeSeconds
            throw SlotAssignmentException(
                eventId = eventId,
                windowsSearched = totalWindows,
                message = "No available window for event $eventId within $totalWindows windows"
            )
        }

        // Occupancy-weighted random selection (lower occupancy = higher chance)
        val picked = windowPicker.pickOccupancyWeightedRandom(candidates, maxSlotsPerWindow)
            ?: throw SlotAssignmentException(
                eventId = eventId,
                windowsSearched = candidates.size.toLong(),
                message = "All candidate windows at capacity for event $eventId"
            )

        val jitterMs = ThreadLocalRandom.current().nextLong(0, windowSizeMs)
        val scheduledTime = picked.plusMillis(jitterMs)

        return claimSlot(eventId, requestedTime, picked, scheduledTime)
    }

    /**
     * INSERT event slot in a single short-lived transaction.
     * No counter write — the background scheduler handles counter updates.
     * Idempotent: duplicate eventId caught by UNIQUE constraint, existing slot re-read.
     */
    private fun claimSlot(
        eventId: String,
        requestedTime: Instant,
        windowStart: Instant,
        scheduledTime: Instant
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

            val delay = Duration.between(requestedTime, scheduledTime).let { d ->
                if (d.isNegative) Duration.ZERO else d
            }
            AssignedSlot(
                eventId = eventId,
                scheduledTime = scheduledTime,
                delay = delay,
                allocationStatus = AllocationStatus.NORMAL
            )
        }
    }
}
