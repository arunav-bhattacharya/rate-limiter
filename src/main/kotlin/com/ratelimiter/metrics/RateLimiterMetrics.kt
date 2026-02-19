package com.ratelimiter.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject

/**
 * MicroProfile/Micrometer metrics for the rate limiter.
 * All meters are registered eagerly so Prometheus scrape sees them before the first event.
 */
@ApplicationScoped
class RateLimiterMetrics @Inject constructor(
    private val registry: MeterRegistry
) {
    /** Total successful slot assignments. */
    val slotsAssigned: Counter = Counter.builder("rate_limiter.slots.assigned")
        .description("Total successful slot assignments")
        .register(registry)

    /** Events that already had an assigned slot (idempotent hits). */
    val idempotentHits: Counter = Counter.builder("rate_limiter.slots.idempotent_hits")
        .description("Event IDs that already had an assigned slot")
        .register(registry)

    /** Windows skipped due to row lock contention (ORA-00054). */
    val windowsContended: Counter = Counter.builder("rate_limiter.windows.contended")
        .description("Windows skipped due to row lock contention")
        .register(registry)

    /** Windows skipped because slot_count >= max_per_window. */
    val windowsFull: Counter = Counter.builder("rate_limiter.windows.full")
        .description("Windows skipped because slot_count >= max_per_window")
        .register(registry)

    /** Events that could not be assigned to any window (lookahead exhausted). */
    val assignmentFailures: Counter = Counter.builder("rate_limiter.slots.failures")
        .description("Events that could not be assigned to any window")
        .register(registry)

    /** ORA-00001 caught on slot insert due to concurrent idempotency race. */
    val idempotencyRaces: Counter = Counter.builder("rate_limiter.slots.idempotency_races")
        .description("ORA-00001 caught on slot insert, re-read existing")
        .register(registry)

    /** Config cache hits. */
    val configCacheHits: Counter = Counter.builder("rate_limiter.config.cache_hits")
        .description("Config loaded from in-memory cache")
        .register(registry)

    /** Config cache misses (DB query required). */
    val configCacheMisses: Counter = Counter.builder("rate_limiter.config.cache_misses")
        .description("Config loaded from database")
        .register(registry)

    /** End-to-end slot assignment duration. */
    val assignmentDuration: Timer = Timer.builder("rate_limiter.assignment.duration")
        .description("Time taken for the full assignSlot operation")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry)
}
