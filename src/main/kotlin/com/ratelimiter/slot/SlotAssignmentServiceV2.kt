package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowEndTrackerRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

@ApplicationScoped
class SlotAssignmentServiceV2 @Inject constructor(
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    private val windowEndTrackerRepository: WindowEndTrackerRepository
) {

    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        return eventSlotRepository.fetchAssignedSlot(eventId) ?: assignNewSlot(eventId, requestedTime)
    }

    private fun assignNewSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        // Get the current window end to bound our search
        val currWindowEnd = currentWindowEnd(requestedTime)

        return transaction {
            val firstAvailableWindow = with(windowSlotCounterRepository) {
                fetchFirstWindowHavingAvailableSlot(requestedTime, currWindowEnd, MAX_SLOTS_PER_WINDOW)
            }

            if (firstAvailableWindow != null) {
                claimSlot(eventId, requestedTime, firstAvailableWindow)
            } else {
                // No available slots found, add another chunk of windows
                val nextWindowStart = currWindowEnd + WINDOW_SIZE
                val newWindowEnd = nextWindowStart + WINDOW_CHUNK_SIZE
                with(windowSlotCounterRepository) { batchInsertWindows(generateWindows(nextWindowStart)) }
                with(windowEndTrackerRepository) { insertWindowEnd(requestedTime, newWindowEnd) }

                // Try to claim a slot again after loading a new chunk of windows
                val window = with(windowSlotCounterRepository) {
                    fetchFirstWindowHavingAvailableSlot(nextWindowStart, newWindowEnd, MAX_SLOTS_PER_WINDOW)
                } ?: throw RuntimeException(
                    "Couldn't find a slot to assign for eventId: " +
                            "$eventId, requestedTime: $requestedTime. No existing windows, and failed to assign in new window."
                )
                claimSlot(eventId, requestedTime, window)
            }
        }
    }

    private fun currentWindowEnd(requestedTime: Instant): Instant {
        return windowEndTrackerRepository.fetchWindowEnd(requestedTime) ?: run {
            // No existing windows for this requested time — insert a chunk
            val windowEnd = requestedTime + WINDOW_CHUNK_SIZE
            transaction {
                with(windowSlotCounterRepository) { batchInsertWindows(generateWindows(requestedTime)) }
                with(windowEndTrackerRepository) { insertWindowEnd(requestedTime, windowEnd) }
            }
            windowEnd
        }
    }

    private fun generateWindows(start: Instant): List<Instant> {
        return (0 until MAX_WINDOWS_IN_CHUNK).map { i ->
            start.plus(WINDOW_SIZE.multipliedBy(i))
        }
    }

    /**
     * Insert the event slot, increment the window counter, and return the assigned slot.
     * If the event already exists (duplicate key), return the existing slot without
     * touching the counter.
     */
    private fun Transaction.claimSlot(
        eventId: String,
        requestedTime: Instant,
        window: Instant
    ): AssignedSlot {
        val jitterMs = ThreadLocalRandom.current().nextLong(0, WINDOW_SIZE.toMillis())
        val scheduledTime = window.plusMillis(jitterMs)

        val inserted = with(eventSlotRepository) {
            insertEventSlot(eventId, requestedTime, window, scheduledTime, DEFAULT_CONFIG_ID)
        }

        if (!inserted) {
            // Idempotent hit — another thread already inserted this event
            return with(eventSlotRepository) { queryAssignedSlot(eventId) }
                ?: throw RuntimeException("Failed to fetch existing slot for eventId: $eventId")
        }

        // Successfully inserted — increment the window counter
        with(windowSlotCounterRepository) { incrementSlotCount(window) }

        val delay = Duration.between(requestedTime, scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }

    companion object {
        private val WINDOW_SIZE = Duration.ofSeconds(4)
        private const val MAX_SLOTS_PER_WINDOW = 10
        private const val DEFAULT_CONFIG_ID = 0L
        private val CONFIG_MAX_WINDOWS_IN_CHUNK = 1000L
        private val MAX_WINDOWS_IN_CHUNK =
            minOf(CONFIG_MAX_WINDOWS_IN_CHUNK, 100L) // enforce an upper bound to limit Oracle batch inserts
        private val WINDOW_CHUNK_SIZE = WINDOW_SIZE.multipliedBy(MAX_WINDOWS_IN_CHUNK)
    }
}
