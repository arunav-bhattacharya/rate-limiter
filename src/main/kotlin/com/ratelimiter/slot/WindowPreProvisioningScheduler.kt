package com.ratelimiter.slot

import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

/**
 * Pre-provisions window counter rows so the hot path never needs to
 * provision on-demand. Invoked by the Temporal pre-provision workflow
 * (daily schedule + startup one-shot + on-demand triggerAsync calls).
 *
 * Uses the static window size from application.yaml (no config table).
 * Determines where provisioning left off via MAX(WNDW_STRT_TS) from RL_WNDW_CT.
 *
 * Idempotent: safe to run on all pods concurrently — duplicate keys are
 * caught silently by [WindowSlotCounterRepository.batchInsertWindows].
 */
@ApplicationScoped
class WindowPreProvisioningScheduler(
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    @param:ConfigProperty(name = "rate-limiter.window-size-seconds", defaultValue = "30")
    private val windowSizeSeconds: Long,
    @param:ConfigProperty(name = "rate-limiter.pre-provision-days", defaultValue = "90")
    private val preProvisionDays: Long,
    @param:ConfigProperty(name = "rate-limiter.pre-provision-batch-size", defaultValue = "5000")
    private val batchSize: Int
) {
    private val logger = LoggerFactory.getLogger(WindowPreProvisioningScheduler::class.java)
    private val windowSize: Duration = Duration.ofSeconds(windowSizeSeconds)

    fun provisionWindows() {
        val now = Instant.now()
        val alignedNow = alignToWindow(now)
        val provisionEnd = alignToWindow(now.plus(Duration.ofDays(preProvisionDays)))

        // Determine where to start: after the last provisioned window, or from now
        val maxProvisioned = transaction {
            with(windowSlotCounterRepository) { fetchMaxProvisionedWindow() }
        }
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
