package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V4 slot assignment — conditional INSERT, no locks, no counter table.
 *
 * Replaces V3's lock+update pattern with a simpler approach:
 * 1. Find the starting window via a DB query (skips past full windows)
 * 2. Compute scan upper bound from max assigned window + headroom
 * 3. Random pick within that range, fallback to sequential scan
 *
 * Uses a soft fill threshold (default 90%) — windows are treated as "full"
 * before reaching exact capacity. Over-capacity is tolerated.
 *
 * Uses static config from application.yaml (no config table).
 * Assumes requestedTime is always window-aligned (no alignment needed).
 */
@ApplicationScoped
class SlotAssignmentServiceV4(
    private val eventSlotRepository: EventSlotRepository,
    @param:ConfigProperty(name = "rate-limiter.max-per-window", defaultValue = "100")
    private val maxPerWindow: Int,
    @param:ConfigProperty(name = "rate-limiter.window-size-seconds", defaultValue = "30")
    private val windowSizeSeconds: Long,
    @param:ConfigProperty(name = "rate-limiter.headroom-windows", defaultValue = "100")
    private val headroomWindows: Long,
    @param:ConfigProperty(name = "rate-limiter.window-fill-threshold", defaultValue = "0.9")
    private val windowFillThreshold: Double
) {
    private val windowSize: Duration = Duration.ofSeconds(windowSizeSeconds)
    private val windowSizeMs: Long = windowSizeSeconds * 1000

    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        // Idempotency check (own transaction, ~1ms)
        val existing = eventSlotRepository.fetchAssignedSlot(eventId)
        if (existing != null) return existing

        val softMax = Math.floor(maxPerWindow * windowFillThreshold).toInt()

        // Compute scan upper bound: max assigned window + headroom.
        val maxWindowStartTime = eventSlotRepository.fetchMaxWindowStartForRequestedTime(requestedTime)
        val upperBound = (maxWindowStartTime ?: requestedTime).plus(windowSize.multipliedBy(headroomWindows))

        // Find starting window via DB query (jumps past full windows)
        val startWindow = findStartWindow(requestedTime, softMax) ?: requestedTime

        // Number of windows to try within the bounded range
        val windowsToTry = maxOf(1L, Duration.between(startWindow, upperBound).toSeconds() / windowSizeSeconds).toInt()

        // 1. Random pick within range — distributes concurrent threads
        val randomOffset = ThreadLocalRandom.current().nextInt(windowsToTry)
        val randomWindow = startWindow.plus(windowSize.multipliedBy(randomOffset.toLong()))
        val randomResult = tryClaimSlot(eventId, randomWindow, softMax, requestedTime)
        if (randomResult != null) return randomResult

        // 2. Fallback: sequential scan through the range (skip the already-tried random window)
        for (attempt in 0 until windowsToTry) {
            if (attempt == randomOffset) continue
            val windowStart = startWindow.plus(windowSize.multipliedBy(attempt.toLong()))
            val result = tryClaimSlot(eventId, windowStart, softMax, requestedTime)
            if (result != null) return result
        }

        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = windowsToTry.toLong(),
            message = "Could not assign slot for event $eventId after $windowsToTry attempts"
        )
    }

    private fun tryClaimSlot(
        eventId: String,
        windowStart: Instant,
        softMax: Int,
        requestedTime: Instant
    ): AssignedSlot? {
        // Step 1: Check capacity
        val count = transaction {
            with(eventSlotRepository) { countSlotsInWindow(windowStart) }
        }
        if (count >= softMax) return null

        // Step 2: Plain INSERT + jitter
        val jitterMs = ThreadLocalRandom.current().nextLong(0, windowSizeMs)
        val scheduledTime = windowStart.plusMillis(jitterMs)

        return transaction {
            with(eventSlotRepository) {
                insertAndReturnSlot(eventId, windowStart, scheduledTime, SlotAssignmentServiceV3.STATIC_CONFIG_ID, requestedTime)
            }
        }
    }

    private fun findStartWindow(requestedTime: Instant, softMax: Int): Instant? {
        val frontier = transaction {
            with(eventSlotRepository) { findFrontierWindow(requestedTime) }
        } ?: return null

        return if (frontier.count < softMax) frontier.windowStart
        else frontier.windowStart.plus(windowSize)
    }
}
