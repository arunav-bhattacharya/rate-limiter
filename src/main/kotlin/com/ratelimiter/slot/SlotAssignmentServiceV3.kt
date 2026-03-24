package com.ratelimiter.slot

import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadLocalRandom

/**
 * V3 slot assignment — Kotlin/Exposed implementation.
 *
 * Assumes requestedTime is always aligned to a window boundary.
 *
 * Relies on [WindowPreProvisioningScheduler] to have pre-provisioned window
 * counter rows in advance. The hot path is:
 * 1. Idempotency check
 * 2. Compute scan upper bound from max assigned window + headroom
 * 3. Find+lock+claim within that range
 *
 * The scan upper bound is derived from actual slot assignments in RL_EVENT_SLOT_DTL:
 * MAX(WNDW_STRT_TS) WHERE REQ_TS = requestedTime, plus [headroomWindows] of headroom.
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
    @param:ConfigProperty(name = "rate-limiter.max-per-window", defaultValue = "100")
    private val maxPerWindow: Int,
    @param:ConfigProperty(name = "rate-limiter.window-size-seconds", defaultValue = "30")
    private val windowSizeSeconds: Long,
    @param:ConfigProperty(name = "rate-limiter.headroom-windows", defaultValue = "100")
    private val headroomWindows: Long
) {
    private val windowSizeMs: Long = windowSizeSeconds * 1000
    private val windowSize: Duration = Duration.ofSeconds(windowSizeSeconds)

    /** JVM-level cache: requestedTime → max WNDW_STRT_TS with actual slot assignments. */
    private val maxWindowCache = ConcurrentHashMap<Instant, Instant>()

    companion object {
        const val STATIC_CONFIG_ID = "STATIC"
    }

    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {

        // Phase 0: Pre-transaction idempotency check (own short-lived transaction).
        val existing = eventSlotRepository.fetchAssignedSlot(eventId)
        if (existing != null) {
            return existing
        }

        // Compute scan upper bound: cache-first, fall through to DB on miss.
        val maxWindowStartTime = maxWindowCache[requestedTime]
            ?: eventSlotRepository.fetchMaxWindowStartForRequestedTime(requestedTime)
                ?.also { maxWindowCache[requestedTime] = it }
        val scanEndTime = (maxWindowStartTime ?: requestedTime).plus(windowSize.multipliedBy(headroomWindows))

        // Single find+lock+claim within the bounded range.
        val result = lockWindowAndClaimSlot(eventId, requestedTime, scanEndTime, requestedTime)
        if (result != null) return result

        // Scan exhausted — refresh from DB in case cache was stale, retry once.
        val dbMaxWindowStartTime = eventSlotRepository.fetchMaxWindowStartForRequestedTime(requestedTime)
        if (dbMaxWindowStartTime != null) {
            maxWindowCache.merge(requestedTime, dbMaxWindowStartTime) { oldValue, newValue -> maxOf(oldValue, newValue) }
            val refreshedEndTime = dbMaxWindowStartTime.plus(windowSize.multipliedBy(headroomWindows))
            if (refreshedEndTime > scanEndTime) {
                val retry = lockWindowAndClaimSlot(eventId, requestedTime, refreshedEndTime, requestedTime)
                if (retry != null) return retry
            }
        }

        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = headroomWindows,
            message = "No available window for event $eventId within headroom of $headroomWindows windows"
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
            claimSlot(eventId, lockedWindow, jitterMs, requestedTime).also {
                maxWindowCache.merge(requestedTime, lockedWindow) { oldValue, newValue -> maxOf(oldValue, newValue) }
            }
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
}
