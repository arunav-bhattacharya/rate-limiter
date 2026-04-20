package com.ratelimiter.slot

import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.ConfigProvider
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.sql.Timestamp
import java.sql.Types
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * Core rate limiting algorithm (V1/V2 PL/SQL implementation).
 *
 * Assigns scheduling slots to payment events by walking through time windows.
 * The entire slot assignment — idempotency check, window walk loop, lock acquisition,
 * slot insertion, and counter update — executes as a single anonymous PL/SQL block
 * in one JDBC round trip.
 *
 * Uses static config from application.yaml (no config table).
 */
@ApplicationScoped
class SlotAssignmentService {
    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        val windowStart = alignToWindowBoundary(requestedTime, windowSizeSeconds)
        val elapsedMs = elapsedInWindowMs(windowStart, requestedTime)
        val maxFirstWindow = computeEffectiveMax(maxPerWindow, elapsedMs, windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, windowSizeMs)
        val fullJitterMs = computeFullWindowJitterMs(windowSizeMs)
        val headroomSecs = headroomWindows.toLong() * windowSizeSeconds

        val result = executeSlotAssignment(
            eventId, windowStart, requestedTime,
            maxFirstWindow, firstJitterMs, fullJitterMs, headroomSecs
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
                    windowsSearched = result.windowsSearched.toLong(),
                    message = "Could not assign slot for event $eventId after searching " +
                            "${result.windowsSearched} windows"
                )
            }

            else -> error("Unexpected PL/SQL status: ${result.status}")
        }
    }

    private data class SlotAssignmentResult(
        val status: Int,
        val slotId: String,
        val scheduledTime: Instant,
        val windowStart: Instant,
        val windowsSearched: Int
    )

    private fun executeSlotAssignment(
        eventId: String,
        windowStart: Instant,
        requestedTime: Instant,
        maxFirstWindow: Int,
        firstJitterMs: Long,
        fullJitterMs: Long,
        headroomSecs: Long
    ): SlotAssignmentResult {
        return transaction {
            val rawConnection = this.connection.connection as java.sql.Connection
            rawConnection.prepareCall(SlotAssignmentSql.ASSIGN_SLOT_PLSQL).use { cs ->
                cs.setString(1, eventId)
                cs.setTimestamp(2, Timestamp.from(windowStart))
                cs.setTimestamp(3, Timestamp.from(requestedTime))
                cs.setString(4, SlotAssignmentServiceV3.STATIC_CONFIG_ID)
                cs.setInt(5, maxPerWindow)
                cs.setLong(6, windowSizeSeconds)
                cs.setInt(7, maxFirstWindow)
                cs.setLong(8, firstJitterMs)
                cs.setLong(9, fullJitterMs)
                cs.setLong(10, headroomSecs)
                cs.setInt(11, maxSearchChunks)

                cs.registerOutParameter(12, Types.INTEGER)
                cs.registerOutParameter(13, Types.VARCHAR)
                cs.registerOutParameter(14, Types.TIMESTAMP)
                cs.registerOutParameter(15, Types.TIMESTAMP)
                cs.registerOutParameter(16, Types.INTEGER)

                cs.execute()

                SlotAssignmentResult(
                    status = cs.getInt(12),
                    slotId = cs.getString(13) ?: "",
                    scheduledTime = cs.getTimestamp(14)?.toInstant() ?: Instant.EPOCH,
                    windowStart = cs.getTimestamp(15)?.toInstant() ?: Instant.EPOCH,
                    windowsSearched = cs.getInt(16)
                )
            }
        }
    }

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

    companion object {
        private val logger = LoggerFactory.getLogger(SlotAssignmentService::class.java)
        private val config = ConfigProvider.getConfig()

        private val maxPerWindow: Int =
            config.getOptionalValue("rate-limiter.max-per-window", Int::class.javaObjectType).orElse(100)
        private val windowSizeSeconds: Long =
            config.getOptionalValue("rate-limiter.window-size-seconds", Long::class.javaObjectType).orElse(30L)
        private val headroomWindows: Int =
            config.getOptionalValue("rate-limiter.headroom-windows", Int::class.javaObjectType).orElse(100)
        private val maxSearchChunks: Int =
            config.getOptionalValue("rate-limiter.max-search-chunks", Int::class.javaObjectType).orElse(10)

        private val windowSizeMs: Long = windowSizeSeconds * 1000
    }
}
