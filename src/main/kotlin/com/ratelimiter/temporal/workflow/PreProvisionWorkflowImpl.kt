package com.ratelimiter.temporal.workflow

import com.ratelimiter.temporal.activity.PreProvisionActivities
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.Workflow
import java.time.Duration

/**
 * Thin orchestration wrapper — single activity call.
 * All provisioning logic lives in [com.ratelimiter.slot.WindowPreProvisioningScheduler.provisionWindows].
 */
class PreProvisionWorkflowImpl : PreProvisionWorkflow {

    override fun run() {
        val activities = Workflow.newActivityStub(
            PreProvisionActivities::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(2))
                .setRetryOptions(
                    RetryOptions.newBuilder()
                        .setMaximumAttempts(3)
                        .setInitialInterval(Duration.ofSeconds(5))
                        .setBackoffCoefficient(2.0)
                        .build()
                )
                .build()
        )

        activities.provisionWindows()
    }
}
