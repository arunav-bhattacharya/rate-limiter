package com.ratelimiter.slot

/**
 * Thrown when no window could accommodate this event
 * within the dynamic lookahead range.
 */
class SlotAssignmentException(
    val eventId: String,
    val windowsSearched: Int,
    val reason: String = SlotAssignmentSql.REASON_CAPACITY_EXHAUSTED,
    val resumeFromWindow: java.time.Instant? = null,
    val searchLimit: java.time.Instant? = null,
    message: String
) : RuntimeException(message)

/**
 * Thrown when no active rate limit config can be loaded for the given name.
 */
class ConfigLoadException(
    val configName: String,
    message: String
) : RuntimeException(message)

/**
 * Thrown when assignment capacity is exhausted before a DB call is attempted.
 */
class AssignmentOverloadedException(
    val retryAfterMs: Long,
    message: String
) : RuntimeException(message)
