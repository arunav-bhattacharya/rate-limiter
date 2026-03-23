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
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.SQLException
import java.sql.Timestamp
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
     * Conditional INSERT: insert a slot only if the window has fewer than softMax slots.
     * Returns AssignedSlot if inserted, null if window is at/above soft threshold.
     * On duplicate EVENT_ID, returns the existing slot (idempotent).
     */
    private companion object {
        const val ORACLE_UNIQUE_CONSTRAINT_ERROR_CODE = 1
    }

    /**
     * Conditional INSERT: insert a slot only if the window has fewer than softMax slots.
     * Returns AssignedSlot if inserted, null if window is at/above soft threshold.
     * On duplicate EVENT_ID, returns the existing slot (idempotent).
     */
    fun Transaction.conditionalInsertSlot(
        eventId: String,
        windowStart: Instant,
        scheduledTime: Instant,
        softMax: Int,
        configId: String,
        requestedTime: Instant
    ): AssignedSlot? {
        val slotId = UUID.randomUUID().toString()
        val sql = """
            INSERT INTO RL_EVENT_SLOT_DTL
                (WNDW_SLOT_ID, EVENT_ID, REQ_TS, RL_WNDW_CONFIG_ID, WNDW_STRT_TS, COMPUTED_SCHED_TS, CREAT_TS)
            SELECT ?, ?, ?, ?, ?, ?, SYSTIMESTAMP
            FROM DUAL
            WHERE (SELECT COUNT(*) FROM RL_EVENT_SLOT_DTL WHERE WNDW_STRT_TS = ?) < ?
        """.trimIndent()

        val conn = TransactionManager.current().connection.connection as java.sql.Connection

        val rowsInserted = try {
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, slotId)
                stmt.setString(2, eventId)
                stmt.setTimestamp(3, Timestamp.from(requestedTime))
                stmt.setString(4, configId)
                stmt.setTimestamp(5, Timestamp.from(windowStart))
                stmt.setTimestamp(6, Timestamp.from(scheduledTime))
                stmt.setTimestamp(7, Timestamp.from(windowStart))
                stmt.setInt(8, softMax)
                stmt.executeUpdate()
            }
        } catch (e: SQLException) {
            if (e.errorCode == ORACLE_UNIQUE_CONSTRAINT_ERROR_CODE) {
                return queryAssignedSlot(eventId)
            }
            throw e
        }

        if (rowsInserted == 0) return null

        val delay = Duration.between(requestedTime, scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }
}
