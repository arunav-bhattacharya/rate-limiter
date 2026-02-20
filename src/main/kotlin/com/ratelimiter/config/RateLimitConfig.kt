package com.ratelimiter.config

import java.time.Duration
import java.time.Instant

/**
 * Domain model for a rate limit configuration.
 * Immutable value type — thread-safe for caching without synchronization.
 */
data class RateLimitConfig(
    val configId: Long,
    val configName: String,
    val maxPerWindow: Int,

    /**
     * Duration of each time window. Stored in the database as an ISO-8601
     * duration string (e.g., "PT4S") and parsed to [Duration] at load time.
     */
    val windowSize: Duration,

    val effectiveFrom: Instant,
    val isActive: Boolean,
    val createdAt: Instant
) {
    /** Window size in whole seconds (for window boundary alignment and PL/SQL params). */
    val windowSizeSecs: Long
        get() = windowSize.toSeconds()

    /** Window size in milliseconds (for jitter range computation). */
    val windowSizeMs: Long
        get() = windowSize.toMillis()
}
