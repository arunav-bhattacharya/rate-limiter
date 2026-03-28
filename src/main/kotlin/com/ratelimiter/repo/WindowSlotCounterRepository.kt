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
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.javatime.JavaInstantColumnType
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.statements.StatementType
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.update
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.sql.Timestamp
import java.time.Instant

@ApplicationScoped
class WindowSlotCounterRepository(
    @param:ConfigProperty(name = "rate-limiter.lock-query-timeout-seconds", defaultValue = "2")
    private val lockQueryTimeoutSeconds: Int
) {

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
     * Finds the earliest available window in [from, to) and locks exactly one row.
     * Simple range scan without CASE — `SLOT_CT < ?` is fully sargable against the
     * composite index `(WNDW_STRT_TS, SLOT_CT)`.
     *
     * Uses Oracle JDBC fetchSize=1 + rowPrefetch=1 to control cursor advancement —
     * Oracle's FOR UPDATE SKIP LOCKED processes rows lazily through the cursor,
     * skipping locked rows server-side and returning the first successfully locked
     * row. Only that one row is locked.
     */
    fun Transaction.lockFirstAvailableWindow(
        from: Instant,
        to: Instant,
        maxSlots: Int
    ): Instant? {
        val rawConn = TransactionManager.current().connection.connection as java.sql.Connection
        val conn = rawConn.unwrap(OracleConnection::class.java)

        val stmt = conn.prepareStatement(
            """
            SELECT /*+ FIRST_ROWS(1) */ WNDW_STRT_TS
            FROM   RL_WNDW_CT
            WHERE  WNDW_STRT_TS >= ?
            AND    WNDW_STRT_TS < ?
            AND    SLOT_CT < ?
            ORDER BY WNDW_STRT_TS ASC
            FOR UPDATE SKIP LOCKED
            """
        ).apply {
            fetchSize = 1
            queryTimeout = lockQueryTimeoutSeconds
            (this as OraclePreparedStatement).rowPrefetch = 1
            setTimestamp(1, Timestamp.from(from))
            setTimestamp(2, Timestamp.from(to))
            setInt(3, maxSlots)
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

    /**
     * Lightweight hint read: finds the earliest window in [from, to) with
     * SLOT_CT < maxSlots. No row lock acquired — result may be stale.
     */
    fun findFirstAvailableWindow(from: Instant, to: Instant, maxSlots: Int): Instant? {
        return org.jetbrains.exposed.sql.transactions.transaction {
            exec(
                """
                SELECT WNDW_STRT_TS
                FROM   RL_WNDW_CT
                WHERE  WNDW_STRT_TS >= ?
                AND    WNDW_STRT_TS < ?
                AND    SLOT_CT < ?
                ORDER BY WNDW_STRT_TS ASC
                FETCH FIRST 1 ROW ONLY
                """.trimIndent(),
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
    }

    /**
     * Returns the last provisioned window timestamp.
     * Used by the pre-provisioner to determine where provisioning left off.
     */
    fun fetchMaxProvisionedWindow(): Instant? {
        return org.jetbrains.exposed.sql.transactions.transaction {
            WindowCounterTable
                .select(WindowCounterTable.windowStart)
                .orderBy(WindowCounterTable.windowStart, org.jetbrains.exposed.sql.SortOrder.DESC)
                .limit(1)
                .firstOrNull()
                ?.get(WindowCounterTable.windowStart)
        }
    }

    fun Transaction.incrementSlotCount(windowStart: Instant) {
        WindowCounterTable.update({ WindowCounterTable.windowStart eq windowStart }) {
            with(SqlExpressionBuilder) {
                it[slotCount] = slotCount + 1
            }
        }
    }

    // ==================== V5 methods ====================

    /**
     * Atomically increments the counter and returns the new value.
     * Uses Oracle PL/SQL RETURNING INTO to get the post-increment count
     * in the same round-trip as the UPDATE.
     *
     * If the row doesn't exist (cold start for this window), creates it with count=1.
     * Must be called within an existing transaction.
     */
    fun Transaction.upsertCounterReturningCount(windowStart: Instant): Int {
        val rawConn = TransactionManager.current().connection.connection as java.sql.Connection
        val conn = rawConn.unwrap(OracleConnection::class.java)

        // Try UPDATE first — common path (counter row already exists)
        val cs = conn.prepareCall(
            "BEGIN UPDATE RL_WNDW_CT SET SLOT_CT = SLOT_CT + 1 WHERE WNDW_STRT_TS = ? RETURNING SLOT_CT INTO ?; END;"
        )
        cs.use { stmt ->
            stmt.setTimestamp(1, Timestamp.from(windowStart))
            stmt.registerOutParameter(2, java.sql.Types.INTEGER)
            stmt.execute()

            // Check if UPDATE affected any row (if no row exists, OUT param is 0/null)
            val newCount = stmt.getInt(2)
            if (!stmt.wasNull()) return newCount
        }

        // Row doesn't exist — cold start. INSERT with count=1.
        val now = Instant.now()
        try {
            WindowCounterTable.insert {
                it[WindowCounterTable.windowStart] = windowStart
                it[slotCount] = 1
                it[createdAt] = now
            }
            return 1
        } catch (e: ExposedSQLException) {
            if (!e.isDuplicateKeyViolation()) throw e
            // Race: another pod created it. Retry UPDATE.
            return upsertCounterReturningCount(windowStart)
        }
    }

    /**
     * Advisory read: returns approximate occupancy for all windows in [from, to)
     * that have counter rows. Windows without rows are treated as count=0 by callers.
     * Runs in its own short-lived read-only transaction (~1ms).
     */
    fun readOccupancy(from: Instant, to: Instant): Map<Instant, Int> {
        return org.jetbrains.exposed.sql.transactions.transaction {
            WindowCounterTable
                .selectAll()
                .where {
                    (WindowCounterTable.windowStart greaterEq from) and
                            (WindowCounterTable.windowStart less to)
                }
                .associate {
                    it[WindowCounterTable.windowStart] to it[WindowCounterTable.slotCount]
                }
        }
    }

    /**
     * UPDATE-first upsert: increments the counter for the given window.
     * If the row doesn't exist (cold start for this window), creates it.
     * Must be called within an existing transaction.
     */
    fun Transaction.upsertCounter(windowStart: Instant) {
        val updated = WindowCounterTable.update({ WindowCounterTable.windowStart eq windowStart }) {
            with(SqlExpressionBuilder) {
                it[slotCount] = slotCount + 1
            }
        }

        if (updated == 0) {
            val now = Instant.now()
            try {
                WindowCounterTable.insert {
                    it[WindowCounterTable.windowStart] = windowStart
                    it[slotCount] = 1
                    it[createdAt] = now
                }
            } catch (e: ExposedSQLException) {
                if (!e.isDuplicateKeyViolation()) throw e
                // Another thread created it concurrently — retry the UPDATE
                WindowCounterTable.update({ WindowCounterTable.windowStart eq windowStart }) {
                    with(SqlExpressionBuilder) {
                        it[slotCount] = slotCount + 1
                    }
                }
            }
        }
    }
}
