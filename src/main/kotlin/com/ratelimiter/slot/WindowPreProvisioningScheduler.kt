package com.ratelimiter.slot

import com.ratelimiter.repo.WindowSlotCounterRepository
import io.quarkus.runtime.Startup
import io.quarkus.scheduler.Scheduled
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

/**
 * Pre-provisions window counter rows 30 days in advance so the hot path
 * never needs to provision on-demand.
 *
 * Uses the static window size from application.yaml (no config table).
 * Determines where provisioning left off via MAX(WNDW_STRT_TS) from RL_WNDW_CT.
 *
 * Runs once at startup (async) and daily at 2 AM via Quarkus scheduler.
 * Idempotent: safe to run on all pods concurrently — duplicate keys are
 * caught silently by [WindowSlotCounterRepository.batchInsertWindows].
 */
@Startup
@ApplicationScoped
class WindowPreProvisioningScheduler(
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    @param:ConfigProperty(name = "rate-limiter.window-size-seconds", defaultValue = "30")
    private val windowSizeSeconds: Long,
    @param:ConfigProperty(name = "rate-limiter.pre-provision-days", defaultValue = "30")
    private val preProvisionDays: Long,
    @param:ConfigProperty(name = "rate-limiter.pre-provision-batch-size", defaultValue = "5000")
    private val batchSize: Int,
    @param:ConfigProperty(name = "rate-limiter.use-temporal-scheduler", defaultValue = "false")
    private val useTemporalScheduler: Boolean
) {
    private val logger = LoggerFactory.getLogger(WindowPreProvisioningScheduler::class.java)
    private val windowSize: Duration = Duration.ofSeconds(windowSizeSeconds)

    @PostConstruct
    fun init() {
        if (useTemporalScheduler) {
            logger.info("Temporal scheduler active — skipping Quarkus startup pre-provisioning")
            return
        }

        Thread.startVirtualThread {
            try {
                provisionWindows()
            } catch (e: Exception) {
                logger.error("Startup pre-provisioning failed", e)
            }
        }
    }

    @Scheduled(cron = "0 0 2 * * ?", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun scheduledProvision() {
        if (useTemporalScheduler) return

        try {
            provisionWindows()
        } catch (e: Exception) {
            logger.error("Scheduled pre-provisioning failed", e)
        }
    }

    fun provisionWindows() {
        val now = Instant.now()
        val alignedNow = alignToWindow(now)
        val provisionEnd = alignToWindow(now.plus(Duration.ofDays(preProvisionDays)))

        // Determine where to start: after the last provisioned window, or from now
        val maxProvisioned = windowSlotCounterRepository.fetchMaxProvisionedWindow()
        val provisionStart = if (maxProvisioned != null && maxProvisioned >= alignedNow) {
            // Start one window after the last provisioned one
            maxProvisioned.plus(windowSize)
        } else {
            alignedNow
        }

        if (provisionStart >= provisionEnd) {
            logger.info("Windows already provisioned to {} — skipping", provisionEnd)
            return
        }

        val totalWindows = Duration.between(provisionStart, provisionEnd).toSeconds() / windowSizeSeconds
        logger.info("Provisioning {} windows (windowSize={}s) from {} to {}",
            totalWindows, windowSizeSeconds, provisionStart, provisionEnd)

        var provisioned = 0L
        var batchStart = provisionStart
        while (batchStart < provisionEnd) {
            val batchEnd = minOf(
                batchStart.plus(windowSize.multipliedBy(batchSize.toLong())),
                provisionEnd
            )
            val batchCount = Duration.between(batchStart, batchEnd).toSeconds() / windowSizeSeconds
            val windows = (0 until batchCount).map { i ->
                batchStart.plus(windowSize.multipliedBy(i))
            }

            transaction {
                with(windowSlotCounterRepository) { batchInsertWindows(windows) }
            }

            provisioned += windows.size
            batchStart = batchEnd
        }

        logger.info("Pre-provisioned {} windows", provisioned)
    }

    private fun alignToWindow(instant: Instant): Instant {
        val epochSecond = instant.epochSecond
        val aligned = epochSecond - (epochSecond % windowSizeSeconds)
        return Instant.ofEpochSecond(aligned)
    }
}
