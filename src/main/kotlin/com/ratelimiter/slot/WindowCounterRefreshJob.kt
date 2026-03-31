package com.ratelimiter.slot

import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.time.Instant

/**
 * Core counter refresh logic for V7 — stateless, independently callable.
 *
 * Designed for Temporal readiness: this class contains only business logic
 * with no scheduling concerns. It can be invoked as:
 *   - A Quarkus @Scheduled task (via [WindowCounterRefreshSchedulerV7])
 *   - A Temporal Activity (in a subsequent iteration)
 *
 * Two operations per run:
 *   1. Absolute-count MERGE: sets SLOT_CT to the total slot count for windows
 *      that received new inserts between [since] and [until]. Idempotent —
 *      safe under multi-pod concurrent execution.
 *   2. Status transition: marks windows as FULL when SLOT_CT >= maxSlotsPerWindow
 */
@ApplicationScoped
class WindowCounterRefreshJob(
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    @param:ConfigProperty(name = "rate-limiter.v7.max-slots-per-window", defaultValue = "900")
    private val maxSlotsPerWindow: Int
) {
    /**
     * Refresh counters and update status for windows that received new slots
     * in the [since, until] interval.
     *
     * @param since exclusive lower bound (slots inserted after this timestamp)
     * @param until inclusive upper bound (slots inserted up to this timestamp)
     */
    fun run(since: Instant, until: Instant) {
        windowSlotCounterRepository.refreshCountersDelta(since, until, maxSlotsPerWindow)
    }
}
