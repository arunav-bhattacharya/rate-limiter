package com.ratelimiter.slot

/**
 * Thrown when no window could accommodate this event
 * within the pre-provisioned range.
 */
class SlotAssignmentException(
    val eventId: String,
    val windowsSearched: Long,
    message: String
) : RuntimeException(message)
