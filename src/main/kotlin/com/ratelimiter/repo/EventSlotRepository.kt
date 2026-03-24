package com.ratelimiter.repo

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.isDuplicateKeyViolation
import com.ratelimiter.slot.AssignedSlot
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.javatime.JavaInstantColumnType
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.statements.StatementType
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.SortOrder.DESC
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@ApplicationScoped
class EventSlotRepository {

    /**
     * Idempotency check — look up an existing slot by eventId.
     * Runs in its own transaction (called before the main assignment transaction).
     */
    fun fetchAssignedSlot(eventId: String): AssignedSlot? {
        return transaction {
            queryAssignedSlot(eventId)
        }
    }

    /**
     * Query an existing slot within the current transaction.
     * Used in the duplicate-key recovery path inside claimSlot.
     */
    fun Transaction.queryAssignedSlot(eventId: String): AssignedSlot? {
        return RateLimitEventSlotTable
            .selectAll()
            .where { RateLimitEventSlotTable.eventId eq eventId }
            .firstOrNull()
            ?.let { row ->
                val requestedTime = row[RateLimitEventSlotTable.requestedTime]
                val scheduledTime = row[RateLimitEventSlotTable.scheduledTime]
                val delay = Duration.between(requestedTime, scheduledTime).let { d ->
                    if (d.isNegative) Duration.ZERO else d
                }
                AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
            }
    }

    /**
     * Insert a new event slot row, managing its own transaction.
     * Used by the REST endpoint (EventSlotResource) for direct inserts.
     */
    fun insertEventSlotInNewTransaction(
        eventId: String,
        requestedTime: Instant,
        windowStart: Instant,
        scheduledTime: Instant,
        configId: String
    ): Boolean {
        return transaction {
            val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            try {
                RateLimitEventSlotTable.insert {
                    it[slotId] = UUID.randomUUID().toString()
                    it[RateLimitEventSlotTable.eventId] = eventId
                    it[RateLimitEventSlotTable.requestedTime] = requestedTime
                    it[RateLimitEventSlotTable.windowStart] = windowStart
                    it[RateLimitEventSlotTable.scheduledTime] = scheduledTime
                    it[RateLimitEventSlotTable.configId] = configId
                    it[RateLimitEventSlotTable.createdAt] = now
                }
                true
            } catch (e: ExposedSQLException) {
                if (!e.isDuplicateKeyViolation()) throw e
                false
            }
        }
    }

    /**
     * Insert a new event slot row within an existing transaction.
     * Returns true if the row was inserted, false if a duplicate eventId already exists.
     */
    fun Transaction.insertEventSlot(
        eventId: String,
        requestedTime: Instant,
        windowStart: Instant,
        scheduledTime: Instant,
        configId: String
    ): Boolean {
        val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        return try {
            RateLimitEventSlotTable.insert {
                it[slotId] = UUID.randomUUID().toString()
                it[RateLimitEventSlotTable.eventId] = eventId
                it[RateLimitEventSlotTable.requestedTime] = requestedTime
                it[RateLimitEventSlotTable.windowStart] = windowStart
                it[RateLimitEventSlotTable.scheduledTime] = scheduledTime
                it[RateLimitEventSlotTable.configId] = configId
                it[RateLimitEventSlotTable.createdAt] = now
            }
            true
        } catch (e: ExposedSQLException) {
            if (!e.isDuplicateKeyViolation()) throw e
            false
        }
    }

    /**
     * Returns the furthest window that has actual slot assignments for a given requestedTime.
     * Used by V3 and V4 to compute the scan upper bound (maxUsed + headroom).
     */
    fun fetchMaxWindowStartForRequestedTime(requestedTime: Instant): Instant? {
        return transaction {
            RateLimitEventSlotTable
                .select(RateLimitEventSlotTable.windowStart)
                .where { RateLimitEventSlotTable.requestedTime eq requestedTime }
                .orderBy(RateLimitEventSlotTable.windowStart, DESC)
                .limit(1)
                .firstOrNull()
                ?.get(RateLimitEventSlotTable.windowStart)
        }
    }

    // ==================== V4 methods ====================

    data class FrontierResult(val windowStart: Instant, val count: Int)

    /**
     * Find the last window with slots and its fill level for the given requestedTime.
     * Used by V4 to jump past full windows in a single query.
     */
    fun Transaction.findFrontierWindow(requestedTime: Instant): FrontierResult? {
        val sql = """
            SELECT WNDW_STRT_TS, COUNT(*) AS cnt
            FROM RL_EVENT_SLOT_DTL
            WHERE WNDW_STRT_TS = (
                SELECT MAX(WNDW_STRT_TS) FROM RL_EVENT_SLOT_DTL WHERE WNDW_STRT_TS >= ?
            )
            GROUP BY WNDW_STRT_TS
        """.trimIndent()

        return exec(
            sql,
            listOf(Pair(JavaInstantColumnType(), requestedTime)),
            StatementType.SELECT
        ) { rs ->
            if (rs.next()) {
                FrontierResult(
                    windowStart = rs.getTimestamp("WNDW_STRT_TS").toInstant(),
                    count = rs.getInt("cnt")
                )
            } else null
        }
    }

    /**
     * Count slots assigned in a specific window.
     */
    fun Transaction.countSlotsInWindow(windowStart: Instant): Long {
        return RateLimitEventSlotTable
            .selectAll()
            .where { RateLimitEventSlotTable.windowStart eq windowStart }
            .count()
    }

    /**
     * Insert a slot and return AssignedSlot.
     * On duplicate EVENT_ID, returns the existing slot (idempotent).
     * Returns null only if the insert fails for a non-duplicate reason (shouldn't happen).
     */
    fun Transaction.insertAndReturnSlot(
        eventId: String,
        windowStart: Instant,
        scheduledTime: Instant,
        configId: String,
        requestedTime: Instant
    ): AssignedSlot {
        val inserted = insertEventSlot(eventId, requestedTime, windowStart, scheduledTime, configId)

        if (!inserted) {
            return queryAssignedSlot(eventId)
                ?: error("Failed to re-read slot for eventId=$eventId after duplicate key")
        }

        val delay = Duration.between(requestedTime, scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }
}
