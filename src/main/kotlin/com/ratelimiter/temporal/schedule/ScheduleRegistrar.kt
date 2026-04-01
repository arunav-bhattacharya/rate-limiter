package com.ratelimiter.temporal.schedule

import com.ratelimiter.temporal.workflow.PreProvisionWorkflow
import io.temporal.api.enums.v1.ScheduleOverlapPolicy
import io.temporal.client.WorkflowOptions
import io.temporal.client.schedules.Schedule
import io.temporal.client.schedules.ScheduleActionStartWorkflow
import io.temporal.client.schedules.ScheduleAlreadyRunningException
import io.temporal.client.schedules.ScheduleClient
import io.temporal.client.schedules.ScheduleOptions
import io.temporal.client.schedules.SchedulePolicy
import io.temporal.client.schedules.ScheduleSpec
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory

/**
 * Creates the daily pre-provisioning Temporal Schedule.
 *
 * The schedule triggers a [PreProvisionWorkflow] execution daily at 2 AM UTC.
 * Uses [ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP] to prevent concurrent
 * executions if one runs long. Idempotent — safe to call from every pod on startup.
 */
@ApplicationScoped
class ScheduleRegistrar(
    private val scheduleClient: ScheduleClient,
    @param:ConfigProperty(name = "temporal.task-queue", defaultValue = "rate-limiter-jobs")
    private val taskQueue: String,
    @param:ConfigProperty(name = "temporal.pre-provision.schedule-id", defaultValue = "pre-provision-daily")
    private val scheduleId: String,
    @param:ConfigProperty(name = "temporal.pre-provision.cron", defaultValue = "0 2 * * *")
    private val cronExpression: String
) {
    private val logger = LoggerFactory.getLogger(ScheduleRegistrar::class.java)

    fun ensurePreProvisionSchedule() {
        val schedule = Schedule.newBuilder()
            .setAction(
                ScheduleActionStartWorkflow.newBuilder()
                    .setWorkflowType(PreProvisionWorkflow::class.java)
                    .setOptions(
                        WorkflowOptions.newBuilder()
                            .setTaskQueue(taskQueue)
                            .build()
                    )
                    .build()
            )
            .setSpec(
                ScheduleSpec.newBuilder()
                    .setCronExpressions(listOf(cronExpression))
                    .build()
            )
            .setPolicy(
                SchedulePolicy.newBuilder()
                    .setOverlap(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP)
                    .build()
            )
            .build()

        try {
            scheduleClient.createSchedule(
                scheduleId,
                schedule,
                ScheduleOptions.newBuilder().build()
            )
            logger.info("Created pre-provision schedule: {}", scheduleId)
        } catch (e: ScheduleAlreadyRunningException) {
            logger.info("Pre-provision schedule already exists: {}", scheduleId)
        }
    }
}
