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
 * All possible outcomes of attempting to claim a slot within a single time window.
 * Sealed hierarchy forces exhaustive `when` expressions at every call site.
 */
sealed class WindowResult {

    /** Slot was successfully assigned in this window. */
    data class Assigned(val slot: InternalSlot) : WindowResult()

    /** Window is at capacity (slot_count >= max_per_window). */
    data object Full : WindowResult()

    /**
     * Window row is locked by another transaction (ORA-00054)
     * or deadlock was detected (ORA-00060).
     */
    data object Contended : WindowResult()

    /** Idempotent hit — this event_id was already assigned a slot. */
    data class AlreadyAssigned(val existingSlot: InternalSlot) : WindowResult()
}

/**
 * Thrown when no window could accommodate this event
 * within the dynamic lookahead range.
 */
class SlotAssignmentException(
    val eventId: String,
    val windowsSearched: Int,
    message: String
) : RuntimeException(message)

/**
 * Thrown when no active rate limit config can be loaded for the given name.
 */
class ConfigLoadException(
    val configName: String,
    message: String
) : RuntimeException(message)
