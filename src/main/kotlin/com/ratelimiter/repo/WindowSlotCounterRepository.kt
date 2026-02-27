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
import java.time.Duration
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
     * Find the earliest window in [from, to) with available capacity. No lock acquired.
     * Uses FETCH FIRST 1 ROW ONLY to read a single row efficiently.
     */
    fun Transaction.findEarliestCandidateWindow(
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
            FETCH FIRST 1 ROW ONLY
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
     * Nested subquery find+lock: inner SELECT finds earliest candidate (no lock,
     * FETCH FIRST 1 ROW ONLY), outer SELECT locks exactly that row by PK
     * (FOR UPDATE SKIP LOCKED) and re-checks capacity under the lock.
     *
     * Returns the locked window's WINDOW_START, or null if no qualifying candidate
     * exists, candidate was locked by another session (SKIP LOCKED), or capacity
     * was lost between subquery and lock (race).
     */
    fun Transaction.nestedFindAndLock(
        from: Instant,
        to: Instant,
        maxSlots: Int
    ): Instant? {
        val sql = """
            SELECT WINDOW_START
            FROM   rate_limit_window_counter
            WHERE  WINDOW_START = (
                SELECT WINDOW_START
                FROM   rate_limit_window_counter
                WHERE  WINDOW_START >= ?
                AND    WINDOW_START < ?
                AND    SLOT_COUNT < ?
                ORDER BY WINDOW_START ASC
                FETCH FIRST 1 ROW ONLY
            )
            AND    SLOT_COUNT < ?
            FOR UPDATE SKIP LOCKED
        """.trimIndent()

        return exec(
            sql,
            listOf(
                Pair(JavaInstantColumnType(), from),
                Pair(JavaInstantColumnType(), to),
                Pair(IntegerColumnType(), maxSlots),
                Pair(IntegerColumnType(), maxSlots)
            ),
            StatementType.SELECT
        ) { rs ->
            if (rs.next()) rs.getTimestamp("WINDOW_START").toInstant() else null
        }
    }

    /**
     * V3-style exclusive-range find+lock: finds the earliest non-full, non-contended
     * window in [from, to) and acquires a row lock on it.
     *
     * Uses a nested subquery for single-SQL find+lock (1 round-trip on success).
     * On null, a non-locking fallback distinguishes "no candidates" (O(1) exit)
     * from "candidate was locked" (advance past it and retry).
     */
    fun Transaction.findAndLockFirstAvailableWindow(
        from: Instant,
        to: Instant,
        maxSlots: Int,
        windowSize: Duration
    ): Instant? {
        var searchFrom = from
        while (searchFrom < to) {
            val result = nestedFindAndLock(searchFrom, to, maxSlots)
            if (result != null) return result
            // null: either no candidates OR candidate was locked (SKIP LOCKED)
            val candidate = findEarliestCandidateWindow(searchFrom, to, maxSlots)
                ?: return null  // truly no candidates → O(1) exit
            searchFrom = candidate.plus(windowSize)  // was locked → advance past it
        }
        return null
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
     * Attempt to lock the first window's counter row and check capacity.
     * Returns:
     *   true  — lock acquired, has capacity (slot_count < maxSlots)
     *   false — lock acquired, full (slot_count >= maxSlots)
     *   null  — row skipped by another session's lock (SKIP LOCKED)
     */
    fun Transaction.tryLockFirstWindow(window: Instant, maxSlots: Int): Boolean? {
        val sql = """
            SELECT SLOT_COUNT
            FROM   rate_limit_window_counter
            WHERE  WINDOW_START = ?
            FOR UPDATE SKIP LOCKED
        """.trimIndent()

        return exec(
            sql,
            listOf(Pair(JavaInstantColumnType(), window)),
            StatementType.SELECT
        ) { rs ->
            if (rs.next()) rs.getInt("SLOT_COUNT") < maxSlots else null
        }
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
