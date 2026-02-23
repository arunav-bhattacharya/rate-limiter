package com.ratelimiter.repo

import com.ratelimiter.db.WindowCounterTable
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.IntegerColumnType
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.javatime.JavaInstantColumnType
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.statements.StatementType
import org.jetbrains.exposed.sql.update
import java.time.Instant

@ApplicationScoped
class WindowSlotCounterRepository {

    /**
     * V2-style inclusive-range find+lock: WHERE window_start >= ? AND window_start <= ?
     */
    fun Transaction.fetchFirstWindowHavingAvailableSlot(
        windowStart: Instant,
        windowEnd: Instant,
        maxSlots: Int
    ): Instant? {
        val sql = """
                    SELECT WINDOW_START
                    FROM   rate_limit_window_counter
                    WHERE
                           WINDOW_START >= ?
                    AND    WINDOW_START <= ?
                    AND    SLOT_COUNT < ?
                    ORDER BY WINDOW_START ASC
                    FOR UPDATE SKIP LOCKED
                """.trimIndent()

        return exec(
            sql,
            listOf(
                Pair(JavaInstantColumnType(), windowStart),
                Pair(JavaInstantColumnType(), windowEnd),
                Pair(IntegerColumnType(), maxSlots)
            ),
            StatementType.SELECT
        ) { rs ->
            if (rs.next()) {
                rs.getTimestamp("WINDOW_START").toInstant()
            } else null
        }
    }

    /**
     * V3-style exclusive-range find+lock: WHERE window_start >= ? AND window_start < ?
     * Atomically finds the earliest non-full, non-contended window in [from, to)
     * and acquires a row lock on it.
     */
    fun Transaction.findAndLockFirstAvailableWindow(
        from: Instant,
        to: Instant,
        maxSlots: Int
    ): Instant? {
        val sql = """
            SELECT WINDOW_START
            FROM   rate_limit_window_counter
            WHERE  WINDOW_START >= ?
            AND    WINDOW_START < ?
            AND    SLOT_COUNT < ?
            ORDER BY WINDOW_START ASC
            FOR UPDATE SKIP LOCKED
        """.trimIndent()

        return exec(
            sql,
            listOf(
                Pair(JavaInstantColumnType(), from),
                Pair(JavaInstantColumnType(), to),
                Pair(IntegerColumnType(), maxSlots)
            ),
            StatementType.SELECT
        ) { rs ->
            if (rs.next()) rs.getTimestamp("WINDOW_START").toInstant() else null
        }
    }

    /**
     * INSERT a window counter row with slot_count=0. Catches duplicate key silently —
     * concurrent threads creating the same window are harmless no-ops.
     */
    fun Transaction.ensureWindowExists(window: Instant) {
        try {
            WindowCounterTable.insert {
                it[windowStart] = window
                it[slotCount] = 0
            }
        } catch (_: ExposedSQLException) {
            // Duplicate key — window already exists
        }
    }

    /**
     * Attempt to lock the first window's counter row only if it has capacity.
     * Returns true if the lock was acquired (capacity available), false if the
     * row is full or locked by another session (SKIP LOCKED).
     */
    fun Transaction.tryLockFirstWindow(window: Instant, maxSlots: Int): Boolean {
        val sql = """
            SELECT WINDOW_START
            FROM   rate_limit_window_counter
            WHERE  WINDOW_START = ?
            AND    SLOT_COUNT < ?
            FOR UPDATE SKIP LOCKED
        """.trimIndent()

        return exec(
            sql,
            listOf(
                Pair(JavaInstantColumnType(), window),
                Pair(IntegerColumnType(), maxSlots)
            ),
            StatementType.SELECT
        ) { rs ->
            rs.next()
        } ?: false
    }

    /**
     * Check if a window counter row exists for the given timestamp.
     */
    fun Transaction.windowExists(window: Instant): Boolean {
        return WindowCounterTable
            .selectAll()
            .where { WindowCounterTable.windowStart eq window }
            .count() > 0
    }

    fun Transaction.batchInsertWindows(windows: List<Instant>) {
        WindowCounterTable.batchInsert(windows, shouldReturnGeneratedValues = false) { window ->
            this[WindowCounterTable.windowStart] = window
            this[WindowCounterTable.slotCount] = 0
        }
    }

    fun Transaction.incrementSlotCount(windowStart: Instant) {
        WindowCounterTable.update({ WindowCounterTable.windowStart eq windowStart }) {
            with(SqlExpressionBuilder) {
                it[slotCount] = slotCount + 1
            }
        }
    }
}
