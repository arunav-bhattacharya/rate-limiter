package com.payments.ratelimiter.slot

import java.time.Instant

/** Result of a successful slot assignment. */
data class AssignedSlot(
    val slotId: Long,
    val eventId: String,
    val windowStart: Instant,
    val slotIndex: Int,
    val scheduledTime: Instant,
    val configId: Long
)

/**
 * All possible outcomes of attempting to claim a slot within a single time window.
 * Sealed hierarchy forces exhaustive `when` expressions at every call site.
 */
sealed class WindowResult {

    /** Slot was successfully assigned in this window. */
    data class Assigned(val slot: AssignedSlot) : WindowResult()

    /** Window is at capacity (slot_count >= max_per_window). */
    data object Full : WindowResult()

    /**
     * Window row is locked by another transaction (ORA-00054)
     * or deadlock was detected (ORA-00060).
     */
    data object Contended : WindowResult()

    /** Idempotent hit — this event_id was already assigned a slot. */
    data class AlreadyAssigned(val existingSlot: AssignedSlot) : WindowResult()
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
