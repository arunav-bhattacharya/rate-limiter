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
     * V2-style inclusive-range find+lock: WHERE WNDW_STRT_TS >= ? AND WNDW_STRT_TS <= ?
     */
    fun Transaction.fetchFirstWindowHavingAvailableSlot(
        windowStart: Instant,
        windowEnd: Instant,
        maxSlots: Int
    ): Instant? {
        val sql = """
                    SELECT WNDW_STRT_TS
                    FROM   RL_WNDW_CT
                    WHERE
                           WNDW_STRT_TS >= ?
                    AND    WNDW_STRT_TS <= ?
                    AND    SLOT_CT < ?
                    ORDER BY WNDW_STRT_TS ASC
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
                rs.getTimestamp("WNDW_STRT_TS").toInstant()
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
            SELECT WNDW_STRT_TS
            FROM   RL_WNDW_CT
            WHERE  WNDW_STRT_TS >= ?
            AND    WNDW_STRT_TS < ?
            AND    SLOT_CT < ?
            ORDER BY WNDW_STRT_TS ASC
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
            if (rs.next()) rs.getTimestamp("WNDW_STRT_TS").toInstant() else null
        }
    }

    /**
     * Nested subquery find+lock: inner SELECT finds earliest candidate (no lock,
     * FETCH FIRST 1 ROW ONLY), outer SELECT locks exactly that row by PK
     * (FOR UPDATE SKIP LOCKED) and re-checks capacity under the lock.
     */
    fun Transaction.nestedFindAndLock(
        from: Instant,
        to: Instant,
        maxSlots: Int
    ): Instant? {
        val sql = """
            SELECT WNDW_STRT_TS
            FROM   RL_WNDW_CT
            WHERE  WNDW_STRT_TS = (
                SELECT WNDW_STRT_TS
                FROM   RL_WNDW_CT
                WHERE  WNDW_STRT_TS >= ?
                AND    WNDW_STRT_TS < ?
                AND    SLOT_CT < ?
                ORDER BY WNDW_STRT_TS ASC
                FETCH FIRST 1 ROW ONLY
            )
            AND    SLOT_CT < ?
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
            if (rs.next()) rs.getTimestamp("WNDW_STRT_TS").toInstant() else null
        }
    }

    /**
     * V3-style exclusive-range find+lock: finds the earliest non-full, non-contended
     * window in [from, to) and acquires a row lock on it.
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
            val candidate = findEarliestCandidateWindow(searchFrom, to, maxSlots)
                ?: return null
            searchFrom = candidate.plus(windowSize)
        }
        return null
    }

    /**
     * INSERT a window counter row with SLOT_CT=0. Catches duplicate key silently.
     */
    fun Transaction.ensureWindowExists(window: Instant) {
        try {
            WindowCounterTable.insert {
                it[windowStart] = window
                it[slotCount] = 0
                it[createdAt] = Instant.now()
            }
        } catch (_: ExposedSQLException) {
            // Duplicate key — window already exists
        }
    }

    /**
     * Attempt to lock the first window's counter row and check capacity.
     */
    fun Transaction.tryLockFirstWindow(window: Instant, maxSlots: Int): Boolean? {
        val sql = """
            SELECT SLOT_CT
            FROM   RL_WNDW_CT
            WHERE  WNDW_STRT_TS = ?
            FOR UPDATE SKIP LOCKED
        """.trimIndent()

        return exec(
            sql,
            listOf(Pair(JavaInstantColumnType(), window)),
            StatementType.SELECT
        ) { rs ->
            if (rs.next()) rs.getInt("SLOT_CT") < maxSlots else null
        }
    }

    fun Transaction.windowExists(window: Instant): Boolean {
        return WindowCounterTable
            .selectAll()
            .where { WindowCounterTable.windowStart eq window }
            .count() > 0
    }

    fun Transaction.batchInsertWindows(windows: List<Instant>) {
        val now = Instant.now()
        WindowCounterTable.batchInsert(windows, shouldReturnGeneratedValues = false) { window ->
            this[WindowCounterTable.windowStart] = window
            this[WindowCounterTable.slotCount] = 0
            this[WindowCounterTable.createdAt] = now
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
