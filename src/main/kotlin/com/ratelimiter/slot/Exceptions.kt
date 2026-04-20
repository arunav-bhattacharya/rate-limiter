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

/** Thrown when the requested rate-limit config name has no active row. */
class ConfigLoadException(
    val configName: String,
    message: String
) : RuntimeException(message)
