package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
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
 * 2. Random pick within a configurable range of windows
 * 3. Fallback to sequential scan if random pick fails
 *
 * Uses a soft fill threshold (default 90%) — windows are treated as "full"
 * before reaching exact capacity. Over-capacity is tolerated.
 *
 * Assumes requestedTime is always window-aligned (no alignment needed).
 */
@ApplicationScoped
class SlotAssignmentServiceV4(
    private val configRepository: RateLimitConfigRepository,
    private val eventSlotRepository: EventSlotRepository,
    @param:ConfigProperty(name = "rate-limiter.max-slot-attempts", defaultValue = "5")
    private val maxSlotAttempts: Int,
    @param:ConfigProperty(name = "rate-limiter.window-fill-threshold", defaultValue = "0.9")
    private val windowFillThreshold: Double
) {

    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")

        // Idempotency check (own transaction, ~1ms)
        val existing = eventSlotRepository.fetchAssignedSlot(eventId)
        if (existing != null) return existing

        val softMax = Math.floor(config.maxPerWindow * windowFillThreshold).toInt()

        // Find starting window via DB query (jumps past full windows)
        val startWindow = findStartWindow(requestedTime, softMax, config) ?: requestedTime

        // 1. Random pick within range — distributes concurrent threads
        val randomOffset = ThreadLocalRandom.current().nextInt(maxSlotAttempts)
        val randomWindow = startWindow.plus(config.windowSize.multipliedBy(randomOffset.toLong()))
        val randomResult = tryClaimSlot(eventId, randomWindow, softMax, config, requestedTime)
        if (randomResult != null) return randomResult

        // 2. Fallback: sequential scan through the range (skip the already-tried random window)
        for (attempt in 0 until maxSlotAttempts) {
            if (attempt == randomOffset) continue
            val windowStart = startWindow.plus(config.windowSize.multipliedBy(attempt.toLong()))
            val result = tryClaimSlot(eventId, windowStart, softMax, config, requestedTime)
            if (result != null) return result
        }

        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = maxSlotAttempts.toLong(),
            message = "Could not assign slot for event $eventId after $maxSlotAttempts attempts"
        )
    }

    private fun tryClaimSlot(
        eventId: String,
        windowStart: Instant,
        softMax: Int,
        config: RateLimitConfig,
        requestedTime: Instant
    ): AssignedSlot? {
        val jitterMs = ThreadLocalRandom.current().nextLong(0, config.windowSizeMs)
        val scheduledTime = windowStart.plusMillis(jitterMs)

        return transaction {
            with(eventSlotRepository) {
                conditionalInsertSlot(eventId, windowStart, scheduledTime, softMax, config.configId, requestedTime)
            }
        }
    }

    private fun findStartWindow(requestedTime: Instant, softMax: Int, config: RateLimitConfig): Instant? {
        val frontier = transaction {
            with(eventSlotRepository) { findFrontierWindow(requestedTime) }
        } ?: return null

        return if (frontier.count < softMax) frontier.windowStart
        else frontier.windowStart.plus(config.windowSize)
    }
}
