package com.ratelimiter.temporal

import com.ratelimiter.slot.WindowCounterRefreshJob
import com.ratelimiter.slot.WindowPreProvisioningScheduler
import com.ratelimiter.temporal.activity.CounterRefreshActivitiesImpl
import com.ratelimiter.temporal.activity.PreProvisionActivitiesImpl
import com.ratelimiter.temporal.schedule.ScheduleRegistrar
import com.ratelimiter.temporal.workflow.CounterRefreshParams
import com.ratelimiter.temporal.workflow.CounterRefreshWorkflow
import com.ratelimiter.temporal.workflow.CounterRefreshWorkflowImpl
import com.ratelimiter.temporal.workflow.PreProvisionWorkflow
import com.ratelimiter.temporal.workflow.PreProvisionWorkflowImpl
import io.quarkus.runtime.Startup
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.worker.WorkerFactory
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory

/**
 * Bridges Quarkus CDI lifecycle with Temporal worker lifecycle.
 *
 * On startup:
 *   1. Creates worker factory and registers workflow + activity types
 *   2. Starts long-polling the Temporal Server for tasks
 *   3. Starts the counter-refresh workflow iff `rate-limiter.use-temporal-scheduler`
 *      is true (avoids double-running with the V7 Quarkus @Scheduled path)
 *   4. Triggers one-shot startup pre-provisioning (unconditional — pre-provision
 *      is Temporal-only now)
 *   5. Ensures the daily pre-provision Schedule exists (unconditional)
 */
@Startup
@ApplicationScoped
class TemporalWorkerStartup(
    private val workflowClient: WorkflowClient,
    private val counterRefreshJob: WindowCounterRefreshJob,
    private val preProvisionScheduler: WindowPreProvisioningScheduler,
    private val scheduleRegistrar: ScheduleRegistrar,
    @param:ConfigProperty(name = "rate-limiter.use-temporal-scheduler", defaultValue = "false")
    private val useTemporalCounterRefresh: Boolean,
    @param:ConfigProperty(name = "temporal.task-queue", defaultValue = "rate-limiter-jobs")
    private val taskQueue: String,
    @param:ConfigProperty(name = "temporal.counter-refresh.workflow-id", defaultValue = "v7-counter-refresh")
    private val counterRefreshWorkflowId: String,
    @param:ConfigProperty(name = "rate-limiter.v7.counter-refresh-every", defaultValue = "3s")
    private val counterRefreshEvery: String,
    @param:ConfigProperty(name = "rate-limiter.v7.counter-refresh-since", defaultValue = "6s")
    private val counterRefreshSince: String
) {
    private val logger = LoggerFactory.getLogger(TemporalWorkerStartup::class.java)
    private var workerFactory: WorkerFactory? = null

    @PostConstruct
    fun init() {
        logger.info("Starting Temporal worker on task queue: {}", taskQueue)

        val factory = WorkerFactory.newInstance(workflowClient)
        val worker = factory.newWorker(taskQueue)

        // Register workflow implementations
        worker.registerWorkflowImplementationTypes(
            CounterRefreshWorkflowImpl::class.java,
            PreProvisionWorkflowImpl::class.java
        )

        // Register activity implementations — manually constructed with CDI-managed deps
        worker.registerActivitiesImplementations(
            CounterRefreshActivitiesImpl(counterRefreshJob),
            PreProvisionActivitiesImpl(preProvisionScheduler)
        )

        factory.start()
        workerFactory = factory
        logger.info("Temporal worker started")

        // V7 counter-refresh is still served by the Quarkus @Scheduled fallback
        // unless explicitly opted in to Temporal via the flag.
        if (useTemporalCounterRefresh) {
            startCounterRefreshWorkflow()
        } else {
            logger.info("Counter-refresh via Quarkus @Scheduled — Temporal counter-refresh workflow skipped")
        }

        // Pre-provisioning is Temporal-only — fire one-shot + ensure daily schedule.
        startPreProvisionWorkflow()
        scheduleRegistrar.ensurePreProvisionSchedule()
    }

    /**
     * Starts the long-running counter-refresh workflow.
     * Uses WorkflowIdConflictPolicy.USE_EXISTING — if already running (from a
     * previous pod or surviving execution), this is a no-op.
     */
    private fun startCounterRefreshWorkflow() {
        try {
            val refreshIntervalSeconds = parseDurationSeconds(counterRefreshEvery)
            val initialLookbackSeconds = parseDurationSeconds(counterRefreshSince)

            val options = WorkflowOptions.newBuilder()
                .setWorkflowId(counterRefreshWorkflowId)
                .setTaskQueue(taskQueue)
                .build()

            val workflow = workflowClient.newWorkflowStub(
                CounterRefreshWorkflow::class.java,
                options
            )

            // Start is idempotent — WorkflowExecutionAlreadyStarted is caught
            WorkflowClient.start(
                workflow::run,
                CounterRefreshParams(
                    initialLookbackSeconds = initialLookbackSeconds,
                    refreshIntervalSeconds = refreshIntervalSeconds
                )
            )
            logger.info("Counter-refresh workflow started: {}", counterRefreshWorkflowId)
        } catch (e: io.temporal.client.WorkflowExecutionAlreadyStarted) {
            logger.info("Counter-refresh workflow already running: {}", counterRefreshWorkflowId)
        } catch (e: Exception) {
            logger.error("Failed to start counter-refresh workflow", e)
        }
    }

    /**
     * Triggers a one-shot pre-provisioning workflow at startup.
     * Uses a timestamped workflow ID to avoid conflicting with the daily schedule.
     */
    private fun startPreProvisionWorkflow() {
        try {
            val workflowId = "pre-provision-startup-${System.currentTimeMillis()}"
            val options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(taskQueue)
                .build()

            val workflow = workflowClient.newWorkflowStub(
                PreProvisionWorkflow::class.java,
                options
            )

            WorkflowClient.start(workflow::run)
            logger.info("Startup pre-provision workflow triggered: {}", workflowId)
        } catch (e: Exception) {
            logger.error("Failed to start startup pre-provision workflow", e)
        }
    }

    /**
     * Parses Quarkus duration strings like "3s", "6s" to seconds.
     */
    private fun parseDurationSeconds(duration: String): Long {
        val trimmed = duration.trim().lowercase()
        return when {
            trimmed.endsWith("s") -> trimmed.dropLast(1).toLong()
            trimmed.endsWith("m") -> trimmed.dropLast(1).toLong() * 60
            else -> trimmed.toLong()
        }
    }

    @PreDestroy
    fun shutdown() {
        workerFactory?.let { factory ->
            logger.info("Shutting down Temporal worker")
            factory.shutdown()
        }
    }
}
