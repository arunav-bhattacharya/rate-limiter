package com.ratelimiter.temporal.workflow

import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * Short-lived workflow for window pre-provisioning.
 * Triggered by a Temporal Schedule (daily cron) and at startup.
 * Shared by V7 and V8.
 */
@WorkflowInterface
interface PreProvisionWorkflow {

    @WorkflowMethod
    fun run()
}
