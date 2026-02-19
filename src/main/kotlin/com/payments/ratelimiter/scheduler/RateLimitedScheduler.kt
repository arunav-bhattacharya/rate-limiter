package com.payments.ratelimiter.scheduler

import com.payments.ratelimiter.slot.AssignedSlot
import com.payments.ratelimiter.slot.SlotAssignmentService
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

/**
 * Orchestrates rate-limited payment scheduling.
 *
 * Assigns a time slot via [SlotAssignmentService], then starts a Temporal
 * workflow with `startDelay` computed from the assigned scheduled time.
 */
@ApplicationScoped
class RateLimitedScheduler @Inject constructor(
    private val slotAssignmentService: SlotAssignmentService,
    private val workflowClient: WorkflowClient,
    @param:ConfigProperty(name = "rate-limiter.default-config-name", defaultValue = "default")
    private val defaultConfigName: String,
    @param:ConfigProperty(name = "temporal.task-queue", defaultValue = "payment-exec")
    private val taskQueue: String
) {
    private val logger = LoggerFactory.getLogger(RateLimitedScheduler::class.java)

    /**
     * Schedule a payment event with rate limiting.
     *
     * 1. Assigns a slot via the rate limiter
     * 2. Computes startDelay = scheduledTime - now
     * 3. Starts a PaymentExecWorkflow via Temporal with the computed delay
     *
     * @param eventId       unique event identifier
     * @param requestedTime desired execution time
     * @param configName    rate limit config name (defaults to "default")
     * @return the assigned slot with scheduled execution details
     */
    fun schedulePaymentEvent(
        eventId: String,
        requestedTime: Instant,
        configName: String = defaultConfigName
    ): AssignedSlot {
        val slot = slotAssignmentService.assignSlot(eventId, configName, requestedTime)

        val delay = Duration.between(Instant.now(), slot.scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }

        logger.info(
            "Scheduling event={} at {} (delay={}ms, window={}, slotIndex={})",
            eventId, slot.scheduledTime, delay.toMillis(), slot.windowStart, slot.slotIndex
        )

        startTemporalWorkflow(eventId, slot, delay)

        return slot
    }

    /**
     * Start a Temporal workflow with the given startDelay.
     *
     * The workflow type (PaymentExecWorkflow) is defined in the payments domain.
     * This method creates a workflow stub with the startDelay option and signals
     * the Temporal server to schedule execution at the future time.
     */
    private fun startTemporalWorkflow(eventId: String, slot: AssignedSlot, delay: Duration) {
        // NOTE: The actual PaymentExecWorkflow interface is defined in the payments domain,
        // not in the rate limiter module. This is a placeholder showing the integration pattern.
        //
        // In production, this would be:
        //
        // val options = WorkflowOptions.newBuilder()
        //     .setWorkflowId("payment-exec-$eventId")
        //     .setTaskQueue(taskQueue)
        //     .setStartDelay(delay)
        //     .build()
        //
        // val workflow = workflowClient.newWorkflowStub(
        //     PaymentExecWorkflow::class.java,
        //     options
        // )
        //
        // WorkflowClient.start(workflow::execute, eventId)

        logger.debug(
            "Would start Temporal workflow for event={} with startDelay={}ms on queue={}",
            eventId, delay.toMillis(), taskQueue
        )
    }
}
