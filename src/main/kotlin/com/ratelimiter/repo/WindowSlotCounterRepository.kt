package com.ratelimiter.repo

import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.db.isDuplicateKeyViolation
import jakarta.enterprise.context.ApplicationScoped
import oracle.jdbc.OracleConnection
import oracle.jdbc.OraclePreparedStatement
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.IntegerColumnType
import java.sql.BatchUpdateException
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.batchInsert
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
     * Finds the earliest available window in [alignedStart, to) and locks exactly one row.
     * Uses a CASE expression to apply proportional capacity to alignedStart
     * and full capacity to all other windows in a single query.
     *
     * Uses Oracle JDBC fetchSize=1 + rowPrefetch=1 to control cursor advancement —
     * Oracle's FOR UPDATE SKIP LOCKED processes rows lazily through the cursor,
     * skipping locked rows server-side and returning the first successfully locked
     * row. Only that one row is locked.
     */
    fun Transaction.findAndLockFirstAvailableWindow(
        from: Instant,
        to: Instant,
        maxFirstWindow: Int,
        maxPerWindow: Int
    ): Instant? {
        val rawConn = TransactionManager.current().connection.connection as java.sql.Connection
        val conn = rawConn.unwrap(OracleConnection::class.java)

        val stmt = conn.prepareStatement(
            """
            SELECT WNDW_STRT_TS
            FROM   RL_WNDW_CT
            WHERE  WNDW_STRT_TS >= ?
            AND    WNDW_STRT_TS < ?
            AND    SLOT_CT < CASE WHEN WNDW_STRT_TS = ? THEN ? ELSE ? END
            ORDER BY WNDW_STRT_TS ASC
            FOR UPDATE SKIP LOCKED
            """
        ).apply {
            fetchSize = 1
            (this as OraclePreparedStatement).rowPrefetch = 1
            setTimestamp(1, Timestamp.from(from))
            setTimestamp(2, Timestamp.from(to))
            setTimestamp(3, Timestamp.from(from))
            setInt(4, maxFirstWindow)
            setInt(5, maxPerWindow)
        }

        return stmt.use { s ->
            val rs = s.executeQuery()
            val result = if (rs.next()) rs.getTimestamp("WNDW_STRT_TS").toInstant() else null
            rs.close()
            result
        }
    }

    fun Transaction.windowExists(window: Instant): Boolean {
        return WindowCounterTable
            .selectAll()
            .where { WindowCounterTable.windowStart eq window }
            .count() > 0
    }

    fun Transaction.batchInsertWindows(windows: List<Instant>) {
        val existing = WindowCounterTable
            .selectAll()
            .where { WindowCounterTable.windowStart inList windows }
            .map { it[WindowCounterTable.windowStart] }
            .toSet()

        val newWindows = windows.filterNot { it in existing }
        if (newWindows.isEmpty()) return

        val now = Instant.now()
        try {
            WindowCounterTable.batchInsert(newWindows, shouldReturnGeneratedValues = false) { window ->
                this[WindowCounterTable.windowStart] = window
                this[WindowCounterTable.slotCount] = 0
                this[WindowCounterTable.createdAt] = now
            }
        } catch (e: BatchUpdateException) {
            if (!e.isDuplicateKeyViolation()) throw e
        } catch (e: ExposedSQLException) {
            if (!e.isDuplicateKeyViolation()) throw e
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
