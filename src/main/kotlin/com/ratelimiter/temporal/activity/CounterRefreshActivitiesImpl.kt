package com.ratelimiter.temporal.activity

import com.ratelimiter.slot.WindowCounterRefreshJob
import java.time.Instant

/**
 * Delegates to [WindowCounterRefreshJob], which is already designed as a
 * stateless, independently callable unit of work (its KDoc explicitly
 * documents Temporal readiness).
 *
 * Not a CDI bean — constructed manually by [com.ratelimiter.temporal.TemporalWorkerStartup],
 * receiving CDI-managed [WindowCounterRefreshJob] as a constructor argument.
 */
class CounterRefreshActivitiesImpl(
    private val job: WindowCounterRefreshJob
) : CounterRefreshActivities {

    override fun refreshCounters(sinceEpochMs: Long, untilEpochMs: Long) {
        job.run(Instant.ofEpochMilli(sinceEpochMs), Instant.ofEpochMilli(untilEpochMs))
    }
}
