package com.ratelimiter.slot

import java.time.Duration
import java.time.Instant

/**
 * Public API response from [SlotAssignmentService.assignSlot].
 *
 * Deliberately minimal — exposes only what the caller needs to act on:
 * the event identity, the scheduled execution time, and how far it was
 * pushed from the originally requested time. Internal details (window
 * boundaries, config IDs, slot row IDs) are not exposed; they are
 * implementation concerns of the rate limiter.
 */
data class AssignedSlot(
    /** The event that was assigned a slot. */
    val eventId: String,

    /** The actual time this event is scheduled to execute. */
    val scheduledTime: Instant,

    /**
     * Duration between the caller's requested time and the actual scheduled time.
     * Always non-negative. Zero means the event was placed in the originally
     * requested window. A positive value means it was pushed forward because
     * earlier windows were full or contended.
     */
    val delay: Duration
)

/**
 * Internal slot result used within the service for window-walk bookkeeping.
 * Contains implementation details (windowStart, slotId, configId) needed
 * for frontier tracking and counter management but not exposed to callers.
 */
internal data class InternalSlot(
    val slotId: Long,
    val eventId: String,
    val windowStart: Instant,
    val scheduledTime: Instant,
    val configId: Long,
    val requestedTime: Instant
) {
    /** Convert to the public API type, computing the delay. */
    fun toAssignedSlot(): AssignedSlot {
        val delay = Duration.between(requestedTime, scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }
        return AssignedSlot(
            eventId = eventId,
            scheduledTime = scheduledTime,
            delay = delay
        )
    }
}

/**
 * Result of [SlotAssignmentService.tryClaimSlotInWindow].
 *
 * Non-null means a slot was obtained (either freshly created or an idempotent hit).
 * Null (returned by the caller convention) means the window was skipped (full or contended).
 *
 * @property slot  the assigned slot details
 * @property isNew true if this slot was just inserted in the current window
 *                 (counter was incremented, frontier should advance).
 *                 false if this is an idempotent re-read of an existing slot
 *                 (counter was NOT touched, frontier should NOT advance).
 */
internal data class ClaimResult(
    val slot: InternalSlot,
    val isNew: Boolean
)
