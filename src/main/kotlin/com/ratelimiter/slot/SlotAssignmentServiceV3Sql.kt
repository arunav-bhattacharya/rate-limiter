package com.ratelimiter.slot

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.sql.Timestamp
import java.sql.Types
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * V3 slot assignment — PL/SQL implementation.
 *
 * Same algorithm as [SlotAssignmentServiceV3] but executes entirely as a single
 * anonymous PL/SQL block in one JDBC round trip. Uses `track_window_end` (append-only)
 * to track the provisioning frontier, then a single find+lock over the entire
 * provisioned range followed by a configurable extension loop from the frontier.
 *
 * Uses static config from application.yaml (no config table).
 */
@ApplicationScoped
class SlotAssignmentServiceV3Sql @Inject constructor(
    @param:ConfigProperty(name = "rate-limiter.max-per-window", defaultValue = "100")
    private val maxPerWindow: Int,
    @param:ConfigProperty(name = "rate-limiter.window-size-seconds", defaultValue = "30")
    private val windowSizeSeconds: Long,
    @param:ConfigProperty(name = "rate-limiter.max-windows-in-chunk", defaultValue = "100")
    private val maxWindowsInChunk: Int,
    @param:ConfigProperty(name = "rate-limiter.max-chunks-to-search", defaultValue = "2")
    private val maxChunksToSearch: Int
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentServiceV3Sql::class.java)
    private val windowSizeMs: Long = windowSizeSeconds * 1000

    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        val totalStart = System.nanoTime()

        val windowStart = alignToWindowBoundary(requestedTime, windowSizeSeconds)
        val elapsedMs = elapsedInWindowMs(windowStart, requestedTime)
        val maxFirstWindow = computeEffectiveMax(maxPerWindow, elapsedMs, windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, windowSizeMs)
        val fullJitterMs = computeFullWindowJitterMs(windowSizeMs)

        val t0 = System.nanoTime()
        val result = executeSlotAssignment(
            eventId, windowStart, requestedTime,
            maxFirstWindow, firstJitterMs, fullJitterMs
        )
        logger.debug("eventId={} | executeSlotAssignment (PL/SQL) took {}ms", eventId, nanosToMs(System.nanoTime() - t0))

        return when (result.status) {
            SlotAssignmentV3Sql.STATUS_NEW -> {
                logger.info(
                    "eventId={} | totalTime={}ms (new, window={}, windowsSearched={})",
                    eventId, nanosToMs(System.nanoTime() - totalStart), result.windowStart, result.windowsSearched
                )
                buildAssignedSlot(eventId, result.scheduledTime, requestedTime)
            }

            SlotAssignmentV3Sql.STATUS_EXISTING -> {
                logger.info("eventId={} | totalTime={}ms (idempotent hit)", eventId, nanosToMs(System.nanoTime() - totalStart))
                buildAssignedSlot(eventId, result.scheduledTime, requestedTime)
            }

            SlotAssignmentV3Sql.STATUS_EXHAUSTED -> {
                logger.warn("eventId={} | totalTime={}ms (exhausted, windowsSearched={})", eventId, nanosToMs(System.nanoTime() - totalStart), result.windowsSearched)
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

    private fun nanosToMs(nanos: Long): String = "%.3f".format(nanos / 1_000_000.0)

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
        fullJitterMs: Long
    ): SlotAssignmentResult {
        return transaction {
            val rawConnection = this.connection.connection as java.sql.Connection
            rawConnection.prepareCall(SlotAssignmentV3Sql.ASSIGN_SLOT_PLSQL).use { cs ->
                // Bind IN parameters (positions 1-11)
                cs.setString(1, eventId)
                cs.setTimestamp(2, Timestamp.from(windowStart))
                cs.setTimestamp(3, Timestamp.from(requestedTime))
                cs.setString(4, SlotAssignmentServiceV3.STATIC_CONFIG_ID)
                cs.setInt(5, maxPerWindow)
                cs.setLong(6, windowSizeSeconds)
                cs.setInt(7, maxFirstWindow)
                cs.setLong(8, firstJitterMs)
                cs.setLong(9, fullJitterMs)
                cs.setInt(10, maxWindowsInChunk)
                cs.setInt(11, maxChunksToSearch)

                // Register OUT parameters (positions 12-16)
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
}

/**
 * SQL constants for V3 PL/SQL slot assignment.
 */
internal object SlotAssignmentV3Sql {
    const val STATUS_NEW = 1
    const val STATUS_EXISTING = 0
    const val STATUS_EXHAUSTED = -1

    val ASSIGN_SLOT_PLSQL: String = SlotAssignmentV3Sql::class.java
        .getResourceAsStream("/assign-slot-v3.sql")
        ?.bufferedReader()
        ?.use { it.readText() }
        ?: throw IllegalStateException("Failed to load assign-slot-v3.sql from resources")
}
