package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.ConfigProvider
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V8 slot assignment — synchronous counters, pre-provisioned windows, proximity-biased selection.
 *
 * Changes from V7:
 *   - Counter update is synchronous (same transaction as slot INSERT), not async
 *   - No WNDW_STATUS flag — filters by SLOT_CT < maxSlots directly
 *   - Proximity+capacity weighted selection (bias toward requestedTime on equal counts)
 *
 * Hot path (2 DB calls):
 *   1. fetchWindowsWithAvailableCapacity — first N windows with SLOT_CT < max
 *   2. claimSlot — INSERT slot + increment counter in single transaction
 *
 * Window lifecycle:
 *   - Pre-provisioned 60 days ahead by [WindowPreProvisioningScheduler]
 *   - Counter incremented synchronously on each slot INSERT
 *
 * If fewer than N candidates exist in the range, proceeds with whatever is available.
 *
 * Assumes requestedTime is always window-aligned.
 * No background counter refresh job required.
 */
@ApplicationScoped
class SlotAssignmentServiceV8(
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    private val windowPicker: WindowPicker,
) {
    /**
     * Assigns a slot for [eventId] within [requestedTime, requestedTime + maxDuration).
     *
     * @param eventId unique event identifier (idempotency key)
     * @param requestedTime desired execution time (must be window-aligned)
     * @param maxDuration maximum delay from requestedTime (required)
     * @throws SlotAssignmentException if no available window exists in the range
     */
    fun assignSlot(eventId: String, requestedTime: Instant, maxDuration: Duration): AssignedSlot {
        val maxDurationEnd = requestedTime.plus(maxDuration)

        // DB call 1: first N pre-provisioned windows with SLOT_CT < max in [requestedTime, maxDurationEnd)
        val candidates = windowSlotCounterRepository.fetchWindowsWithAvailableCapacity(
            requestedTime, maxDurationEnd, maxSlotsPerWindow, candidateWindowCount
        )

        if (candidates.isEmpty()) {
            val totalWindows = maxDuration.toSeconds() / windowSize.toSeconds()
            throw SlotAssignmentException(
                eventId = eventId,
                windowsSearched = totalWindows,
                message = "No available window for event $eventId within $totalWindows windows"
            )
        }

        // Proximity+capacity weighted selection (closer windows with more capacity preferred)
        val windows = candidates.map { it.first }
        val occupancy = candidates.associate { it.first to it.second }

        val picked = windowPicker.pickProximityWeightedRandom(windows, occupancy, maxSlotsPerWindow)
            ?: throw SlotAssignmentException(
                eventId = eventId,
                windowsSearched = candidates.size.toLong(),
                message = "All candidate windows at capacity for event $eventId"
            )

        val jitterMs = ThreadLocalRandom.current().nextLong(0, windowSizeMs)
        val scheduledTime = picked.plusMillis(jitterMs)

        // DB call 2: INSERT slot + synchronous counter increment
        return claimSlot(eventId, requestedTime, picked, scheduledTime)
    }

    /**
     * INSERT event slot + synchronous counter increment in a single transaction.
     * Idempotent: duplicate eventId caught by UNIQUE constraint, existing slot re-read
     * (counter NOT incremented on idempotent replay).
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
                // Duplicate eventId — idempotency (no counter touch)
                return@transaction with(eventSlotRepository) { queryAssignedSlot(eventId) }
                    ?: error("Failed to re-read slot for eventId=$eventId after duplicate key")
            }

            // Synchronous counter increment (row guaranteed to exist via pre-provisioning)
            with(windowSlotCounterRepository) { incrementSlotCount(windowStart) }

            val delay = Duration.between(requestedTime, scheduledTime).let { d ->
                if (d.isNegative) Duration.ZERO else d
            }
            AssignedSlot(
                eventId = eventId,
                scheduledTime = scheduledTime,
                delay = delay
            )
        }
    }

    companion object {
        const val STATIC_CONFIG_ID = "STATIC"

        private val config = ConfigProvider.getConfig()

        private val windowSize: Duration =
            config.getOptionalValue("rate-limiter.v8.window-size", Duration::class.java).orElse(Duration.ofSeconds(30))
        private val maxSlotsPerWindow: Int =
            config.getOptionalValue("rate-limiter.v8.max-slots-per-window", Int::class.javaObjectType).orElse(900)
        private val candidateWindowCount: Int =
            config.getOptionalValue("rate-limiter.v8.candidate-window-count", Int::class.javaObjectType).orElse(30)

        private val windowSizeMs: Long = windowSize.toMillis()
    }
}
