package com.ratelimiter.slot

import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * Quarkus @Scheduled wrapper for [WindowCounterRefreshJob].
 *
 * Manages the scheduling cadence and in-memory [lastSuccessfulRunTs] tracking.
 * On failure, the timestamp is NOT advanced — the next run re-processes the
 * failed interval (at-least-once semantics).
 *
 * Temporal migration path: replace this class with a Temporal Schedule that
 * invokes [WindowCounterRefreshJob.run] as an activity. The since/until
 * window management moves to the Temporal workflow.
 */
@ApplicationScoped
class WindowCounterRefreshSchedulerV7(
    private val job: WindowCounterRefreshJob,
    @param:ConfigProperty(name = "rate-limiter.v7.counter-refresh-since", defaultValue = "6s")
    private val counterRefreshSince: Duration,
    @param:ConfigProperty(name = "rate-limiter.use-temporal-scheduler", defaultValue = "false")
    private val useTemporalScheduler: Boolean
) {
    private val logger = LoggerFactory.getLogger(WindowCounterRefreshSchedulerV7::class.java)
    private val lastSuccessfulRunTs = AtomicReference<Instant>(null)

    @Scheduled(
        every = "\${rate-limiter.v7.counter-refresh-every:3s}",
        concurrentExecution = Scheduled.ConcurrentExecution.SKIP
    )
    fun refresh() {
        if (useTemporalScheduler) return

        try {
            val now = Instant.now()
            val since = lastSuccessfulRunTs.get() ?: now.minus(counterRefreshSince)
            job.run(since, now)
            lastSuccessfulRunTs.set(now)
        } catch (e: Exception) {
            logger.error("V7 counter refresh failed", e)
        }
    }
}
