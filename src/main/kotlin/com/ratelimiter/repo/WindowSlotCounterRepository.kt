package com.ratelimiter.repo

import com.ratelimiter.db.WindowCounterTable
import jakarta.enterprise.context.ApplicationScoped
import oracle.jdbc.OracleConnection
import oracle.jdbc.OraclePreparedStatement
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.IntegerColumnType
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.javatime.JavaInstantColumnType
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.statements.StatementType
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.update
import java.sql.Timestamp
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
            WHERE  WNDW_STRT_TS >= ?
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
            if (rs.next()) rs.getTimestamp("WNDW_STRT_TS").toInstant() else null
        }
    }

    /**
     * Tier 1 (fast path): Finds the earliest available window in [from, to) and
     * locks exactly one row. Uses Oracle JDBC fetchSize=1 + rowPrefetch=1 to
     * control cursor advancement — Oracle's FOR UPDATE SKIP LOCKED processes
     * rows lazily through the cursor, skipping locked rows server-side and
     * returning the first successfully locked row. Only that one row is locked.
     *
     * This is superior to FETCH FIRST 1 ROW ONLY + FOR UPDATE SKIP LOCKED,
     * where Oracle picks 1 candidate before locking — two threads can pick the
     * same candidate, one wins the lock, the other gets empty result. With
     * fetchSize=1, concurrent threads naturally lock different rows because
     * the skip-locked logic runs within the cursor scan.
     */
    fun Transaction.findAndLockFirstAvailableWindow(
        from: Instant,
        to: Instant,
        maxSlots: Int
    ): Instant? {
        val conn = TransactionManager.current().connection.connection as OracleConnection

        val stmt = conn.prepareStatement(
            """
            SELECT WNDW_STRT_TS
            FROM   RL_WNDW_CT
            WHERE  WNDW_STRT_TS >= ?
            AND    WNDW_STRT_TS < ?
            AND    SLOT_CT < ?
            ORDER BY WNDW_STRT_TS ASC
            FOR UPDATE SKIP LOCKED
            """
        ).apply {
            fetchSize = 1
            (this as OraclePreparedStatement).rowPrefetch = 1
            setTimestamp(1, Timestamp.from(from))
            setTimestamp(2, Timestamp.from(to))
            setInt(3, maxSlots)
        }

        // Don't use connection.close as that will close the entire transaction — just close the statement and result set.
        return try {
            val rs = stmt.executeQuery()
            val result = if (rs.next()) rs.getTimestamp("WNDW_STRT_TS").toInstant() else null
            rs.close()
            result
        } finally {
            stmt.close()
        }
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
