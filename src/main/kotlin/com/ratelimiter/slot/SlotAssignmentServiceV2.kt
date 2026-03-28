package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V2 slot assignment — legacy implementation.
 * Uses pre-provisioned windows (no on-demand provisioning).
 * Scan range bounded by headroom windows from max assigned slot.
 */
@ApplicationScoped
class SlotAssignmentServiceV2 @Inject constructor(
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    @param:ConfigProperty(name = "rate-limiter.max-per-window", defaultValue = "100")
    private val maxPerWindow: Int,
    @param:ConfigProperty(name = "rate-limiter.window-size-seconds", defaultValue = "30")
    private val windowSizeSeconds: Long,
    @param:ConfigProperty(name = "rate-limiter.headroom-windows", defaultValue = "100")
    private val headroomWindows: Long
) {
    private val windowSize: Duration = Duration.ofSeconds(windowSizeSeconds)

    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        return eventSlotRepository.fetchAssignedSlot(eventId) ?: assignNewSlot(eventId, requestedTime)
    }

    private fun assignNewSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        val maxUsed = eventSlotRepository.fetchMaxWindowStartTime(requestedTime)
        val scanEnd = (maxUsed ?: requestedTime).plus(windowSize.multipliedBy(headroomWindows))

        return transaction {
            val firstAvailableWindow = with(windowSlotCounterRepository) {
                fetchFirstWindowHavingAvailableSlot(requestedTime, scanEnd, maxPerWindow)
            } ?: throw SlotAssignmentException(
                eventId = eventId,
                windowsSearched = headroomWindows,
                message = "No available window for event $eventId within headroom of $headroomWindows windows"
            )

            claimSlot(eventId, requestedTime, firstAvailableWindow)
        }
    }

    private fun Transaction.claimSlot(
        eventId: String,
        requestedTime: Instant,
        window: Instant
    ): AssignedSlot {
        val jitterMs = ThreadLocalRandom.current().nextLong(0, windowSize.toMillis())
        val scheduledTime = window.plusMillis(jitterMs)

        val inserted = with(eventSlotRepository) {
            insertEventSlot(eventId, requestedTime, window, scheduledTime, SlotAssignmentServiceV3.STATIC_CONFIG_ID)
        }

        if (!inserted) {
            return with(eventSlotRepository) { queryAssignedSlot(eventId) }
                ?: throw RuntimeException("Failed to fetch existing slot for eventId: $eventId")
        }

        with(windowSlotCounterRepository) { incrementSlotCount(window) }

        val delay = Duration.between(requestedTime, scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }
}
