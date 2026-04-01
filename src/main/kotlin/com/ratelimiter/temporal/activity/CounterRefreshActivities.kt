package com.ratelimiter.temporal.activity

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

/**
 * Temporal activity for V7 counter refresh.
 *
 * Uses epoch millis (Long) instead of Instant to avoid Jackson JSR-310
 * serialization issues with Temporal's default data converter.
 */
@ActivityInterface
interface CounterRefreshActivities {

    /**
     * Refresh counters for windows that received new slot inserts in [sinceEpochMs, untilEpochMs].
     * Delegates to [com.ratelimiter.slot.WindowCounterRefreshJob.run].
     */
    @ActivityMethod
    fun refreshCounters(sinceEpochMs: Long, untilEpochMs: Long)
}
