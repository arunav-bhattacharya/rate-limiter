package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.config.RateLimitConfigRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.sql.Timestamp
import java.sql.Types
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/**
 * Core rate limiting algorithm.
 *
 * Slot assignment executes in one PL/SQL round trip with SKIP LOCKED and
 * on-demand horizon extension.
 */
@ApplicationScoped
class SlotAssignmentService @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    @param:ConfigProperty(name = "rate-limiter.headroom-windows", defaultValue = "100")
    private val defaultHeadroomWindows: Int,
    @param:ConfigProperty(name = "rate-limiter.max-inflight-assignments", defaultValue = "25")
    private val maxInflightAssignments: Int,
    @param:ConfigProperty(name = "rate-limiter.inflight-acquire-timeout-ms", defaultValue = "50")
    private val inflightAcquireTimeoutMs: Long,
    @param:ConfigProperty(name = "rate-limiter.horizon-extension-buffer-minutes", defaultValue = "5")
    private val horizonExtensionBufferMinutes: Long
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentService::class.java)
    private val inflightLimiter = Semaphore(maxInflightAssignments, true)

    /**
     * Assign a scheduling slot for the given event.
     *
     * @param resumeFromWindow optional continuation cursor from a previous exhausted response
     */
    fun assignSlot(
        eventId: String,
        configName: String,
        requestedTime: Instant,
        resumeFromWindow: Instant? = null
    ): AssignedSlot {
        val permitAcquired = try {
            inflightLimiter.tryAcquire(inflightAcquireTimeoutMs, TimeUnit.MILLISECONDS)
        } catch (ie: InterruptedException) {
            Thread.currentThread().interrupt()
            throw AssignmentOverloadedException(
                retryAfterMs = inflightAcquireTimeoutMs,
                message = "Interrupted while waiting for slot-assignment capacity"
            )
        }

        if (!permitAcquired) {
            throw AssignmentOverloadedException(
                retryAfterMs = inflightAcquireTimeoutMs,
                message = "Too many concurrent slot-assignment requests"
            )
        }

        try {
            val config = configRepository.loadActiveConfig(configName)
                ?: throw ConfigLoadException(
                    configName,
                    "No active rate limit config found for: $configName"
                )

            val requestedWindowStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
            val alignedResumeWindow = resumeFromWindow
                ?.let { alignToWindowBoundary(it, config.windowSizeSecs) }
                ?: requestedWindowStart
            val effectiveWindowStart = if (alignedResumeWindow > requestedWindowStart) {
                alignedResumeWindow
            } else {
                requestedWindowStart
            }

            val firstWindowIsRequestedWindow = effectiveWindowStart == requestedWindowStart
            val elapsedMs = if (firstWindowIsRequestedWindow) {
                elapsedInWindowMs(requestedWindowStart, requestedTime)
            } else {
                0L
            }

            val maxFirstWindow = if (firstWindowIsRequestedWindow) {
                computeEffectiveMax(config.maxPerWindow, elapsedMs, config.windowSizeMs)
            } else {
                config.maxPerWindow
            }

            val firstJitterMs = if (firstWindowIsRequestedWindow) {
                computeFirstWindowJitterMs(elapsedMs, config.windowSizeMs)
            } else {
                computeFullWindowJitterMs(config.windowSizeMs)
            }

            val fullJitterMs = computeFullWindowJitterMs(config.windowSizeMs)
            val effectiveHeadroomWindows = config.headroomWindows ?: defaultHeadroomWindows
            val headroomSecs = effectiveHeadroomWindows.toLong() * config.windowSizeSecs
            val horizonBufferSecs = horizonExtensionBufferMinutes * 60

            val result = executeSlotAssignment(
                eventId = eventId,
                windowStart = effectiveWindowStart,
                requestedTime = requestedTime,
                config = config,
                maxFirstWindow = maxFirstWindow,
                firstJitterMs = firstJitterMs,
                fullJitterMs = fullJitterMs,
                headroomSecs = headroomSecs,
                horizonBufferSecs = horizonBufferSecs
            )

            return when (result.status) {
                SlotAssignmentSql.STATUS_NEW -> {
                    logger.info(
                        "Assigned slot for eventId={} in window={} after searching {} windows",
                        eventId, result.windowStart, result.windowsSearched
                    )
                    buildAssignedSlot(eventId, result.scheduledTime, requestedTime)
                }

                SlotAssignmentSql.STATUS_EXISTING -> {
                    logger.debug("Idempotent hit for eventId={}", eventId)
                    buildAssignedSlot(eventId, result.scheduledTime, requestedTime)
                }

                SlotAssignmentSql.STATUS_EXHAUSTED -> {
                    throw SlotAssignmentException(
                        eventId = eventId,
                        windowsSearched = result.windowsSearched,
                        reason = result.exhaustReason ?: SlotAssignmentSql.REASON_CAPACITY_EXHAUSTED,
                        resumeFromWindow = result.resumeWindowStart,
                        searchLimit = result.searchLimit,
                        message = "Could not assign slot for event $eventId after searching " +
                                "${result.windowsSearched} windows"
                    )
                }

                else -> error("Unexpected PL/SQL status: ${result.status}")
            }
        } finally {
            inflightLimiter.release()
        }
    }

    /** Marshalling result from the PL/SQL block's OUT parameters. */
    private data class SlotAssignmentResult(
        val status: Int,
        val slotId: Long,
        val scheduledTime: Instant,
        val windowStart: Instant,
        val windowsSearched: Int,
        val resumeWindowStart: Instant?,
        val searchLimit: Instant?,
        val exhaustReason: String?
    )

    /**
     * Execute the PL/SQL slot assignment block via CallableStatement.
     */
    private fun executeSlotAssignment(
        eventId: String,
        windowStart: Instant,
        requestedTime: Instant,
        config: RateLimitConfig,
        maxFirstWindow: Int,
        firstJitterMs: Long,
        fullJitterMs: Long,
        headroomSecs: Long,
        horizonBufferSecs: Long
    ): SlotAssignmentResult {
        return transaction {
            val rawConnection = this.connection.connection as java.sql.Connection
            rawConnection.prepareCall(SlotAssignmentSql.ASSIGN_SLOT_PLSQL).use { cs ->
                cs.setString(1, eventId)
                cs.setTimestamp(2, Timestamp.from(windowStart))
                cs.setTimestamp(3, Timestamp.from(requestedTime))
                cs.setLong(4, config.configId)
                cs.setInt(5, config.maxPerWindow)
                cs.setLong(6, config.windowSizeSecs)
                cs.setInt(7, maxFirstWindow)
                cs.setLong(8, firstJitterMs)
                cs.setLong(9, fullJitterMs)
                cs.setLong(10, headroomSecs)
                cs.setLong(11, horizonBufferSecs)

                cs.registerOutParameter(12, Types.INTEGER)
                cs.registerOutParameter(13, Types.BIGINT)
                cs.registerOutParameter(14, Types.TIMESTAMP)
                cs.registerOutParameter(15, Types.TIMESTAMP)
                cs.registerOutParameter(16, Types.INTEGER)
                cs.registerOutParameter(17, Types.TIMESTAMP)
                cs.registerOutParameter(18, Types.TIMESTAMP)
                cs.registerOutParameter(19, Types.VARCHAR)

                cs.execute()

                SlotAssignmentResult(
                    status = cs.getInt(12),
                    slotId = cs.getLong(13),
                    scheduledTime = cs.getTimestamp(14)?.toInstant() ?: Instant.EPOCH,
                    windowStart = cs.getTimestamp(15)?.toInstant() ?: Instant.EPOCH,
                    windowsSearched = cs.getInt(16),
                    resumeWindowStart = cs.getTimestamp(17)?.toInstant(),
                    searchLimit = cs.getTimestamp(18)?.toInstant(),
                    exhaustReason = cs.getString(19)
                )
            }
        }
    }

    /** Build the public [AssignedSlot] from PL/SQL results, computing the delay. */
    private fun buildAssignedSlot(
        eventId: String,
        scheduledTime: Instant,
        requestedTime: Instant
    ): AssignedSlot {
        val delay = Duration.between(requestedTime, scheduledTime).let {
            if (it.isNegative) Duration.ZERO else it
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }

    private fun alignToWindowBoundary(time: Instant, windowSizeSecs: Long): Instant {
        val epochSecond = time.epochSecond
        val alignedEpoch = epochSecond - (epochSecond % windowSizeSecs)
        return Instant.ofEpochSecond(alignedEpoch)
    }

    private fun elapsedInWindowMs(windowStart: Instant, requestedTime: Instant): Long {
        return Duration.between(windowStart, requestedTime).toMillis()
    }

    private fun computeEffectiveMax(maxPerWindow: Int, elapsedMs: Long, windowSizeMs: Long): Int {
        if (elapsedMs <= 0) return maxPerWindow
        val remainingMs = windowSizeMs - elapsedMs
        return Math.floorDiv(maxPerWindow.toLong() * remainingMs, windowSizeMs).toInt()
    }

    private fun computeFirstWindowJitterMs(elapsedMs: Long, windowSizeMs: Long): Long {
        val lowerBound = if (elapsedMs > 0) elapsedMs else 0L
        return ThreadLocalRandom.current().nextLong(lowerBound, windowSizeMs)
    }

    private fun computeFullWindowJitterMs(windowSizeMs: Long): Long {
        return ThreadLocalRandom.current().nextLong(0, windowSizeMs)
    }
}
