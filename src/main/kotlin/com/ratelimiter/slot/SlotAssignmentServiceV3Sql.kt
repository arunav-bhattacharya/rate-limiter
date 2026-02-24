package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.repo.RateLimitConfigRepository
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
 * Parameter 10 is `max_windows_in_chunk` (a count). Parameter 11 is
 * `max_chunks_to_search` (extension iterations, default 2).
 */
@ApplicationScoped
class SlotAssignmentServiceV3Sql @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    @param:ConfigProperty(name = "rate-limiter.max-windows-in-chunk", defaultValue = "100")
    private val maxWindowsInChunk: Int,
    @param:ConfigProperty(name = "rate-limiter.max-chunks-to-search", defaultValue = "2")
    private val maxChunksToSearch: Int
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentServiceV3Sql::class.java)

    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        val totalStart = System.nanoTime()

        var t0 = System.nanoTime()
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(configName, "No active rate limit config found for: $configName")
        logger.debug("eventId={} | loadConfig took {}ms", eventId, nanosToMs(System.nanoTime() - t0))

        val windowStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val elapsedMs = elapsedInWindowMs(windowStart, requestedTime)
        val maxFirstWindow = computeEffectiveMax(config.maxPerWindow, elapsedMs, config.windowSizeMs)
        val firstJitterMs = computeFirstWindowJitterMs(elapsedMs, config.windowSizeMs)
        val fullJitterMs = computeFullWindowJitterMs(config.windowSizeMs)

        t0 = System.nanoTime()
        val result = executeSlotAssignment(
            eventId, windowStart, requestedTime, config,
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
                    windowsSearched = result.windowsSearched,
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
        val slotId: Long,
        val scheduledTime: Instant,
        val windowStart: Instant,
        val windowsSearched: Int
    )

    private fun executeSlotAssignment(
        eventId: String,
        windowStart: Instant,
        requestedTime: Instant,
        config: RateLimitConfig,
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
                cs.setLong(4, config.configId)
                cs.setInt(5, config.maxPerWindow)
                cs.setLong(6, config.windowSizeSecs)
                cs.setInt(7, maxFirstWindow)
                cs.setLong(8, firstJitterMs)
                cs.setLong(9, fullJitterMs)
                cs.setInt(10, maxWindowsInChunk)
                cs.setInt(11, maxChunksToSearch)

                // Register OUT parameters (positions 12-16)
                cs.registerOutParameter(12, Types.INTEGER)
                cs.registerOutParameter(13, Types.BIGINT)
                cs.registerOutParameter(14, Types.TIMESTAMP)
                cs.registerOutParameter(15, Types.TIMESTAMP)
                cs.registerOutParameter(16, Types.INTEGER)

                cs.execute()

                SlotAssignmentResult(
                    status = cs.getInt(12),
                    slotId = cs.getLong(13),
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
