package com.ratelimiter.slot

import com.ratelimiter.repo.WindowSlotCounterRepository
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

/**
 * Background scheduler that reconciles RL_WNDW_CT counters with actual slot
 * counts from RL_EVENT_SLOT_DTL.
 *
 * Uses CREAT_TS-based discovery: finds windows that received recent inserts
 * (regardless of how far in the future WNDW_STRT_TS is), then counts ALL
 * slots for each and MERGEs into the counter table.
 *
 * Works for requestedTimes spanning 1 minute to 30+ days — follows the data,
 * not a fixed time range.
 *
 * With N staggered pods, effective refresh rate = N / interval. At 20 pods
 * with 3s interval, counters refresh every ~150ms on average.
 */
@ApplicationScoped
class WindowCounterRefreshScheduler(
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    @param:ConfigProperty(name = "rate-limiter.v6.counter-refresh-since", defaultValue = "6s")
    private val counterRefreshSince: Duration,
) {
    private val logger = LoggerFactory.getLogger(WindowCounterRefreshScheduler::class.java)

    @Scheduled(
        every = "\${rate-limiter.v6.counter-refresh-every:3s}",
        concurrentExecution = Scheduled.ConcurrentExecution.SKIP
    )
    fun refresh() {
        try {
            val since = Instant.now().minus(counterRefreshSince)
            windowSlotCounterRepository.refreshRecentlyActiveCounters(since)
        } catch (e: Exception) {
            logger.error("Counter refresh failed", e)
        }
    }
}
