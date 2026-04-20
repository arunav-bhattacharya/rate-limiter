package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.ConfigProvider
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.floor

/**
 * V4 slot assignment — optimistic INSERT, no row locks, no counter table.
 *
 * DB calls per request:
 *   1. Idempotency check        — indexed lookup on EVENT_ID
 *   2. Per-requestedTime frontier — MAX(WNDW_STRT_TS) WHERE REQ_TS = ?
 *   3. Full-windows blocklist    — global GROUP BY HAVING on [requestedTime, computedEnd)
 *   4. Slot insert               — single INSERT
 *
 * Scan boundaries are per-requestedTime (each requestedTime discovers capacity
 * independently). Capacity counting is global (shared windows across requestedTimes).
 */
@ApplicationScoped
class SlotAssignmentServiceV4(
    private val eventSlotRepository: EventSlotRepository,
) {
    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        // 1. Idempotency
        val existing = eventSlotRepository.fetchAssignedSlot(eventId)
        if (existing != null) return existing

        val softMax = floor(maxPerWindow * windowFillThreshold).toInt()

        // 2. Scan boundaries — per-requestedTime frontier
        val currMaxWindow = eventSlotRepository.fetchMaxWindowStartTime(requestedTime)
        val initialEndWindow = maxOf(
            requestedTime,
            currMaxWindow?.plus(windowSize) ?: requestedTime
        )
        val totalInInitialRange = if (initialEndWindow > requestedTime) {
            Duration.between(requestedTime, initialEndWindow).toSeconds() / windowSizeSeconds
        } else 0L

        // 3. Blocklist — global capacity check (counts slots from ALL requestedTimes)
        val fullWindowsInInitialRange =
            eventSlotRepository.findFullWindowsInRange(requestedTime, initialEndWindow, softMax)
        val availableInInitialRange = totalInInitialRange - fullWindowsInInitialRange.size

        // 4. Adaptive extension — only extend when available capacity is below threshold
        val computedEndWindow =
            if (availableInInitialRange >= (headroomWindows * headroomCapacityThreshold).toLong()) {
                initialEndWindow
            } else {
                initialEndWindow.plus(windowSize.multipliedBy(headroomWindows))
            }

        // 5. Build available windows
        //    Windows in [requestedTime, initialEndWindow] are filtered against the blocklist.
        //    Windows in [initialEndWindow, computedEndWindow] have no slots — all available.
        val availableWindows = generateSequence(requestedTime) { it + windowSize }
            .takeWhile { it < computedEndWindow }
            .filter { it !in fullWindowsInInitialRange }
            .toList()

        if (availableWindows.isEmpty()) {
            throw SlotAssignmentException(
                eventId = eventId,
                windowsSearched = totalInInitialRange,
                message = "Could not assign slot for event $eventId — all windows at capacity"
            )
        }

        // 6. Random pick + jitter + insert
        val windowStart = availableWindows[ThreadLocalRandom.current().nextInt(availableWindows.size)]
        val scheduledTime = windowStart.plusMillis(ThreadLocalRandom.current().nextLong(0, windowSizeMs))

        return eventSlotRepository.insertAndReturnSlot(
            eventId, windowStart, scheduledTime,
            SlotAssignmentServiceV3.STATIC_CONFIG_ID, requestedTime
        )
    }

    companion object {
        private val config = ConfigProvider.getConfig()

        private val maxPerWindow: Int =
            config.getOptionalValue("rate-limiter.max-per-window", Int::class.javaObjectType).orElse(100)
        private val windowSizeSeconds: Long =
            config.getOptionalValue("rate-limiter.window-size-seconds", Long::class.javaObjectType).orElse(30L)
        private val headroomWindows: Long =
            config.getOptionalValue("rate-limiter.headroom-windows", Long::class.javaObjectType).orElse(100L)
        private val windowFillThreshold: Double =
            config.getOptionalValue("rate-limiter.window-fill-threshold", Double::class.javaObjectType).orElse(0.9)
        private val headroomCapacityThreshold: Double =
            config.getOptionalValue("rate-limiter.headroom-capacity-threshold", Double::class.javaObjectType).orElse(0.5)

        private val windowSize: Duration = Duration.ofSeconds(windowSizeSeconds)
        private val windowSizeMs: Long = windowSizeSeconds * 1000
    }
}
