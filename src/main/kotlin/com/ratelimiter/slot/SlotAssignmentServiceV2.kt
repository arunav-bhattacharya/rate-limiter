package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
import com.ratelimiter.repo.WindowChunkFrontierRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V2 slot assignment — chunked, frontier-tracked, runtime-config driven.
 *
 * Unified loop: iteration 0 reads (or provisions) the initial chunk for
 * `requestedTime` via the frontier tracker; subsequent iterations extend the
 * frontier by another chunk. Each iteration runs a short find+lock+claim
 * transaction over the extended range.
 *
 * Public signature unchanged from prior V2: `assignSlot(eventId, requestedTime)`.
 * Config is loaded internally via [RateLimitConfigRepository] using a
 * constructor-injected name (default "default").
 */
@ApplicationScoped
class SlotAssignmentServiceV2 @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    private val eventSlotRepository: EventSlotRepository,
    private val windowSlotCounterRepository: WindowSlotCounterRepository,
    private val windowChunkFrontierRepository: WindowChunkFrontierRepository,
    @param:ConfigProperty(name = "rate-limiter.config-name", defaultValue = "default")
    private val configName: String,
    @ConfigProperty(name = "rate-limiter.max-windows-in-chunk", defaultValue = "100")
    rawMaxWindowsInChunk: Long,
    @ConfigProperty(name = "rate-limiter.max-chunks-to-search", defaultValue = "2")
    rawMaxChunksToSearch: Int,
    @param:ConfigProperty(name = "rate-limiter.query-timeout-seconds", defaultValue = "3")
    private val queryTimeoutSeconds: Int,
) {
    private val maxWindowsInChunk: Long = rawMaxWindowsInChunk.coerceIn(1, 100)
    private val maxChunksToSearch: Int = rawMaxChunksToSearch.coerceIn(1, 4)

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
        var provisionFrom = requestedTime

        for (iteration in 0 until maxChunksToSearch) {
            val chunkEnd: Instant = if (iteration == 0) {
                fetchOrProvisionChunk(requestedTime, windowSize)
            } else {
                val extensionEnd = provisionFrom.plus(windowSize.multipliedBy(maxWindowsInChunk))
                rateLimiterTransaction {
                    provisionChunkAndExtendFrontier(provisionFrom, windowSize, requestedTime, extensionEnd)
                }
                extensionEnd
            }

            findWindowAndClaimSlot(eventId, requestedTime, chunkEnd, requestedTime, config)
                ?.let { return it }

            provisionFrom = chunkEnd
        }

        val searched = maxChunksToSearch * maxWindowsInChunk
        throw SlotAssignmentException(
            eventId = eventId,
            windowsSearched = searched,
            message = "Could not assign slot for event $eventId after searching $searched windows"
        )
    }

    private fun fetchOrProvisionChunk(requestedTime: Instant, windowSize: Duration): Instant {
        windowChunkFrontierRepository.fetchMaxWindowEndCached(requestedTime)?.let { return it }

        return rateLimiterTransaction {
            with(windowChunkFrontierRepository) { fetchMaxWindowEndFromDb(requestedTime) }
                ?.let { return@rateLimiterTransaction it }

            val chunkEnd = requestedTime.plus(windowSize.multipliedBy(maxWindowsInChunk))
            provisionChunkAndExtendFrontier(requestedTime, windowSize, requestedTime, chunkEnd)
            chunkEnd
        }
    }

    private fun findWindowAndClaimSlot(
        eventId: String,
        scanStart: Instant,
        lastWindow: Instant,
        requestedTime: Instant,
        config: RateLimitConfig,
    ): AssignedSlot? = rateLimiterTransaction {
        val locked = with(windowSlotCounterRepository) {
            lockFirstAvailableWindow(scanStart, lastWindow, config.maxPerWindow)
        } ?: return@rateLimiterTransaction null

        val jitterMs = ThreadLocalRandom.current().nextLong(0, config.windowSizeMs)
        claimSlot(eventId, locked, requestedTime, config.configId, jitterMs)
    }

    private fun Transaction.provisionChunkAndExtendFrontier(
        from: Instant,
        windowSize: Duration,
        requestedTime: Instant,
        chunkEnd: Instant,
    ) {
        with(windowSlotCounterRepository) {
            val lastWindow = from.plus(windowSize.multipliedBy(maxWindowsInChunk - 1))
            if (!windowExists(lastWindow)) {
                val windows = (0 until maxWindowsInChunk).map { from.plus(windowSize.multipliedBy(it)) }
                batchInsertWindows(windows)
            }
        }
        with(windowChunkFrontierRepository) { insertWindowFrontier(requestedTime, chunkEnd) }
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
}
