package com.ratelimiter.temporal.workflow

import com.ratelimiter.temporal.activity.CounterRefreshActivities
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ActivityFailure
import io.temporal.workflow.Workflow
import java.time.Duration

/**
 * Long-running workflow with a sleep loop.
 *
 * State management:
 *   - [lastSuccessfulRunEpochMs] is workflow-level state, surviving replays and pod failures
 *   - On activity success: timestamp advanced to `now`
 *   - On activity failure (all retries exhausted): timestamp NOT advanced — next iteration
 *     retries the same interval (at-least-once semantics, matching the old Quarkus scheduler)
 *   - continueAsNew every ~50 minutes (1000 iterations * 3s) carries the timestamp forward
 *
 * Determinism:
 *   - Uses [Workflow.currentTimeMillis] instead of Instant.now() — replay-safe
 *   - Uses [Workflow.sleep] instead of Thread.sleep — timer-based, durable
 */
class CounterRefreshWorkflowImpl : CounterRefreshWorkflow {

    private val logger = Workflow.getLogger(CounterRefreshWorkflowImpl::class.java)

    override fun run(params: CounterRefreshParams) {
        val activities = Workflow.newActivityStub(
            CounterRefreshActivities::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(10))
                .setRetryOptions(
                    RetryOptions.newBuilder()
                        .setMaximumAttempts(3)
                        .setInitialInterval(Duration.ofSeconds(1))
                        .setBackoffCoefficient(2.0)
                        .build()
                )
                .build()
        )

        var lastSuccessfulRunEpochMs = params.lastSuccessfulRunEpochMs
        var iterations = 0

        while (iterations < params.maxIterationsBeforeContinueAsNew) {
            Workflow.sleep(Duration.ofSeconds(params.refreshIntervalSeconds))

            val now = Workflow.currentTimeMillis()
            val since = lastSuccessfulRunEpochMs
                ?: (now - params.initialLookbackSeconds * 1000)

            try {
                activities.refreshCounters(since, now)
                lastSuccessfulRunEpochMs = now
            } catch (e: ActivityFailure) {
                // Timestamp NOT advanced — next iteration retries same interval
                logger.warn("Counter refresh failed, will retry next iteration", e)
            }

            iterations++
        }

        // Carry durable state into the new execution
        Workflow.continueAsNew(
            params.copy(lastSuccessfulRunEpochMs = lastSuccessfulRunEpochMs)
        )
    }
}
