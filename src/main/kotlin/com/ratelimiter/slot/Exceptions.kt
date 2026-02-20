package com.ratelimiter.slot

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
