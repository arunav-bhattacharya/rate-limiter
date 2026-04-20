package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.ConfigProvider
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
 * Relies on [WindowPreProvisioningScheduler] to have pre-provisioned window
 * counter rows in advance. The hot path is:
 * 1. Idempotency check
 * 2. Lightweight read to find first available window (no lock)
 * 3. Lock+claim within a narrow range around the hint
 *
 * The lightweight read avoids scanning through full windows with FOR UPDATE
 * SKIP LOCKED — it jumps directly to the first window with capacity.
 *
 * DB work is split across multiple short-lived transactions to minimize
 * connection hold time and reduce pool contention under high TPS.
 *
 * Uses raw SQL for lock queries (Exposed DSL doesn't support FOR UPDATE
 * SKIP LOCKED) and Exposed DSL for inserts and updates.
 */
@ApplicationScoped
class SlotAssignmentServiceV3(
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
) {
    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {

        // Phase 0: Pre-transaction idempotency check (own short-lived transaction).
        val existing = eventSlotRepository.fetchAssignedSlot(eventId)
        if (existing != null) {
            return existing
        }

        var searchEnd = requestedTime.plus(windowSize.multipliedBy(headroomWindows))

        // Bounded retry loop: hint read → lock+claim → extend search range on failure.
        for (iteration in 0..maxRetryIterations) {
            // Hint read: find first available window (no lock, own transaction).
            val hint = windowSlotCounterRepository
                .findFirstAvailableWindow(requestedTime, searchEnd, maxPerWindow)
                ?: break // All windows in range are full

            // Lock + claim within narrow range around the hint.
            val lockEnd = minOf(hint.plus(lockHeadroom), searchEnd)
            val result = lockWindowAndClaimSlot(eventId, hint, lockEnd, requestedTime)
            if (result != null) return result

            // Narrow range exhausted under contention — extend search range.
            val maxAssigned = eventSlotRepository.fetchMaxWindowStartTime(requestedTime)
            if (maxAssigned != null) {
                searchEnd = maxOf(searchEnd, maxAssigned.plus(windowSize.multipliedBy(headroomWindows)))
            }
        }

        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = headroomWindows,
            message = "No available window for event $eventId after $maxRetryIterations retries"
        )
    }

    // ---- Split-transaction helpers ----

    /**
     * Find an available window, lock it, and claim the slot — all in one short transaction.
     * Single sargable range query with full capacity (SLOT_CT < maxPerWindow).
     */
    private fun lockWindowAndClaimSlot(
        eventId: String,
        scanStartTime: Instant,
        scanEndTime: Instant,
        requestedTime: Instant
    ): AssignedSlot? {
        return transaction {
            val lockedWindow = with(windowSlotCounterRepository) {
                lockFirstAvailableWindow(scanStartTime, scanEndTime, maxPerWindow)
            } ?: return@transaction null

            val jitterMs = ThreadLocalRandom.current().nextLong(0, windowSizeMs)
            claimSlot(eventId, lockedWindow, jitterMs, requestedTime)
        }
    }

    /**
     * Insert event slot + increment window counter. Handles duplicate event_id
     * via catch (idempotency: re-reads existing slot without touching the counter).
     */
    private fun Transaction.claimSlot(
        eventId: String,
        window: Instant,
        jitterMs: Long,
        requestedTime: Instant
    ): AssignedSlot {
        val scheduledTime = window.plusMillis(jitterMs)

        val inserted = with(eventSlotRepository) {
            insertEventSlot(eventId, requestedTime, window, scheduledTime, STATIC_CONFIG_ID)
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

    companion object {
        const val STATIC_CONFIG_ID = "STATIC"

        private val config = ConfigProvider.getConfig()

        private val maxPerWindow: Int =
            config.getOptionalValue("rate-limiter.max-per-window", Int::class.javaObjectType).orElse(900)
        private val windowSizeSeconds: Long =
            config.getOptionalValue("rate-limiter.window-size-seconds", Long::class.javaObjectType).orElse(30L)
        private val headroomWindows: Long =
            config.getOptionalValue("rate-limiter.headroom-windows", Long::class.javaObjectType).orElse(600L)
        private val lockHeadroomWindows: Long =
            config.getOptionalValue("rate-limiter.lock-headroom-windows", Long::class.javaObjectType).orElse(50L)
        private val maxRetryIterations: Int =
            config.getOptionalValue("rate-limiter.max-retry-iterations", Int::class.javaObjectType).orElse(4)

        private val windowSizeMs: Long = windowSizeSeconds * 1000
        private val windowSize: Duration = Duration.ofSeconds(windowSizeSeconds)
        private val lockHeadroom: Duration = windowSize.multipliedBy(lockHeadroomWindows)
    }
}
