package com.ratelimiter.temporal.workflow

import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * Long-running workflow that refreshes V7 window counters on a fixed cadence.
 *
 * Runs as a single persistent workflow instance (one per cluster). Stores
 * [CounterRefreshParams.lastSuccessfulRunEpochMs] as durable workflow state —
 * survives pod crashes, replays, and restarts. Uses [io.temporal.workflow.Workflow.sleep]
 * between iterations and [io.temporal.workflow.Workflow.continueAsNew] to bound history.
 */
@WorkflowInterface
interface CounterRefreshWorkflow {

    @WorkflowMethod
    fun run(params: CounterRefreshParams)
}

/**
 * Parameters for the counter refresh workflow.
 * Uses Long (epoch millis) for timestamps to avoid Jackson serialization issues.
 */
data class CounterRefreshParams(
    /** Lookback duration (seconds) when no prior successful run exists. */
    val initialLookbackSeconds: Long = 6,
    /** Sleep duration (seconds) between refresh iterations. */
    val refreshIntervalSeconds: Long = 3,
    /** Max iterations before continueAsNew to bound history growth. */
    val maxIterationsBeforeContinueAsNew: Int = 1000,
    /** Epoch millis of last successful refresh — carried across continueAsNew. */
    val lastSuccessfulRunEpochMs: Long? = null
)
