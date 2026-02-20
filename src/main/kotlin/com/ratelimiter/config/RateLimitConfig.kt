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
    val windowSizeSecs: Int,
    val effectiveFrom: Instant,
    val isActive: Boolean,
    val createdAt: Instant
) {
    /** Window size as a Duration for time arithmetic. */
    val windowDuration: Duration
        get() = Duration.ofSeconds(windowSizeSecs.toLong())

    /** Window size in milliseconds for jitter computation. */
    val windowSizeMs: Long
        get() = windowSizeSecs * 1000L
}
