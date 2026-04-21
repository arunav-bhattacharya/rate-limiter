package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
import com.ratelimiter.repo.WindowChunkFrontierRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import com.ratelimiter.temporal.schedule.ScheduleRegistrar
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.ConfigProvider
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V2 slot assignment — frontier-tracked, backed by a background Temporal
 * pre-provisioner.
 *
 * Window provisioning is handled exclusively by [WindowPreProvisioningScheduler]
 * via the daily Temporal Schedule (plus on-demand triggers from this service).
 * The hot path here never inserts into `RL_WNDW_CT`; it only reads + appends to
 * `RL_WNDW_FRONTIER_TRK`.
 *
 * Flow (mirrors the original V2 loop, minus the batch window insert):
 *   1. Read `MAX(WNDW_END_TS)` from the frontier for this `requestedTime`.
 *   2. If absent, create an initial frontier entry at
 *      `requestedTime + windowSize * maxWindowsInChunk`.
 *   3. Try find+lock+claim in `[requestedTime, chunkEnd)`.
 *   4. On miss, extend the frontier by `windowSize * maxWindowsInChunk` and
 *      retry, up to `maxChunksToSearch` times.
 *   5. After exhaustion, trigger an out-of-band pre-provision run and throw
 *      `SlotAssignmentException`.
 */
@ApplicationScoped
class SlotAssignmentServiceV2(
    private val configRepository: RateLimitConfigRepository,
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    private val windowChunkFrontierRepository: WindowChunkFrontierRepository,
    private val scheduleRegistrar: ScheduleRegistrar,
) {
    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        eventSlotRepository.fetchAssignedSlot(eventId)?.let { return it }

        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")

        return doSlotAssignment(eventId, requestedTime, config)
    }

    private fun doSlotAssignment(
        eventId: String,
        requestedTime: Instant,
        config: RateLimitConfig
    ): AssignedSlot {
        val windowSize = config.windowSize
        var chunkEnd: Instant = fetchOrCreateFrontier(requestedTime, windowSize)

        for (iteration in 0 until maxChunksToSearch) {
            findWindowAndClaimSlot(eventId, requestedTime, chunkEnd, config)
                ?.let { return it }

            // No window in [requestedTime, chunkEnd] — extend frontier by one chunk and retry.
            chunkEnd = extendFrontier(requestedTime, chunkEnd, windowSize)
        }

        // Final retry against the last extension.
        findWindowAndClaimSlot(eventId, requestedTime, chunkEnd, config)
            ?.let { return it }

        scheduleRegistrar.triggerAsync()
        val searched = (maxChunksToSearch + 1) * maxWindowsInChunk
        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = searched,
            message = "Could not assign slot for event $eventId after searching $searched windows; scheduler triggered"
        )
    }

    /**
     * Returns the current frontier max-end for `requestedTime`, creating an
     * initial row at `requestedTime + windowSize * maxWindowsInChunk` if none
     * exists. Windows for that range are already present thanks to the
     * background pre-provisioner.
     */
    private fun fetchOrCreateFrontier(requestedTime: Instant, windowSize: Duration): Instant =
        rateLimiterTransaction {
            with(windowChunkFrontierRepository) { fetchMaxWindowEnd(requestedTime) }
                ?.let { return@rateLimiterTransaction it }

            val chunkEnd = requestedTime.plus(windowSize.multipliedBy(maxWindowsInChunk))
            with(windowChunkFrontierRepository) { insertWindowFrontier(requestedTime, chunkEnd) }
            chunkEnd
        }

    /**
     * Appends a new frontier row at `currentMaxEnd + windowSize * maxWindowsInChunk`
     * and returns the new end. No window batch-insert — the background scheduler
     * is responsible for keeping `RL_WNDW_CT` populated ahead of this frontier.
     */
    private fun extendFrontier(
        requestedTime: Instant,
        currentMaxEnd: Instant,
        windowSize: Duration,
    ): Instant {
        val newEnd = currentMaxEnd.plus(windowSize.multipliedBy(maxWindowsInChunk))
        rateLimiterTransaction {
            with(windowChunkFrontierRepository) { insertWindowFrontier(requestedTime, newEnd) }
        }
        return newEnd
    }

    private fun findWindowAndClaimSlot(
        eventId: String,
        requestedTime: Instant,
        chunkEnd: Instant,
        config: RateLimitConfig,
    ): AssignedSlot? = rateLimiterTransaction {
        val locked = with(windowSlotCounterRepository) {
            lockFirstAvailableWindow(requestedTime, chunkEnd, config.maxPerWindow)
        } ?: return@rateLimiterTransaction null

        val jitterMs = ThreadLocalRandom.current().nextLong(0, config.windowSizeMs)
        claimSlot(eventId, locked, requestedTime, config.configId, jitterMs)
    }

    private fun Transaction.claimSlot(
        eventId: String,
        window: Instant,
        requestedTime: Instant,
        configId: String,
        jitterMs: Long,
    ): AssignedSlot {
        val scheduledTime = window.plusMillis(jitterMs)

        val inserted = with(eventSlotRepository) {
            insertEventSlot(eventId, requestedTime, window, scheduledTime, configId)
        }

        if (!inserted) {
            return with(eventSlotRepository) { queryAssignedSlot(eventId) }
                ?: error("Failed to re-read slot for eventId=$eventId after duplicate key")
        }

        with(windowSlotCounterRepository) { incrementSlotCount(window) }

        val delay = Duration.between(requestedTime, scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }

    /**
     * Force READ_COMMITTED. Exposed defaults to REPEATABLE_READ which Oracle maps to
     * SERIALIZABLE; that would freeze the snapshot at txn start and break claimSlot's
     * duplicate-key re-read path (the row a peer just committed would be invisible).
     */
    private inline fun <T> rateLimiterTransaction(crossinline block: Transaction.() -> T): T =
        transaction(transactionIsolation = Connection.TRANSACTION_READ_COMMITTED, db = null) {
            queryTimeout = queryTimeoutSeconds
            block()
        }

    companion object {
        private val config = ConfigProvider.getConfig()

        private val configName: String =
            config.getOptionalValue("rate-limiter.config-name", String::class.java).orElse("default")
        private val queryTimeoutSeconds: Int =
            config.getOptionalValue("rate-limiter.query-timeout-seconds", Int::class.javaObjectType).orElse(3)

        private val maxWindowsInChunk: Long =
            config.getOptionalValue("rate-limiter.max-windows-in-chunk", Long::class.javaObjectType)
                .orElse(100L).coerceIn(1, 100)
        private val maxChunksToSearch: Int =
            config.getOptionalValue("rate-limiter.max-chunks-to-search", Int::class.javaObjectType)
                .orElse(2).coerceIn(1, 4)
    }
}
