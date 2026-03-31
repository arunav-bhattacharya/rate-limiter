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
import org.jetbrains.exposed.sql.transactions.transaction
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
        return transaction {
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
        return transaction {
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

    /**
     * Advisory read: returns approximate occupancy for all windows in [from, to)
     * that have counter rows. Windows without rows are treated as count=0 by callers.
     * Runs in its own short-lived read-only transaction (~1ms).
     */
    fun readOccupancy(from: Instant, to: Instant): Map<Instant, Int> {
        return transaction {
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
    // ==================== V6 methods ====================

    /**
     * Refresh counters for windows that received slot inserts since [since].
     * Uses CREAT_TS index to discover recently active windows (regardless of how
     * far in the future WNDW_STRT_TS is), then counts ALL slots for each.
     *
     * At 500 TPS with 3s refresh interval (since = now - 6s):
     *   Inner query: ~3000 recent slots → ~50-100 distinct windows
     *   Outer query: ~50-100 windows × ~900 slots = ~45-90K rows to COUNT
     *   MERGE: ~50-100 upserts
     * All fast on Oracle with existing indexes.
     */
    fun refreshRecentlyActiveCounters(since: Instant) {
        transaction {
            val conn = TransactionManager.current().connection.connection as java.sql.Connection
            conn.prepareStatement(
                """
                MERGE INTO RL_WNDW_CT tgt
                USING (
                    SELECT d.WNDW_STRT_TS, COUNT(*) cnt
                    FROM RL_EVENT_SLOT_DTL d
                    WHERE d.WNDW_STRT_TS IN (
                        SELECT DISTINCT WNDW_STRT_TS
                        FROM RL_EVENT_SLOT_DTL
                        WHERE CREAT_TS >= ?
                    )
                    GROUP BY d.WNDW_STRT_TS
                ) src ON (tgt.WNDW_STRT_TS = src.WNDW_STRT_TS)
                WHEN MATCHED THEN UPDATE SET SLOT_CT = src.cnt
                WHEN NOT MATCHED THEN INSERT (WNDW_STRT_TS, SLOT_CT, CREAT_TS)
                    VALUES (src.WNDW_STRT_TS, src.cnt, SYSTIMESTAMP)
                """.trimIndent()
            ).use { stmt ->
                stmt.setTimestamp(1, Timestamp.from(since))
                stmt.executeUpdate()
            }
        }
    }

    // ==================== V7 methods ====================

    /**
     * Returns up to [limit] windows with STATUS = 'AVAILABLE' in [from, to),
     * ordered by WNDW_STRT_TS ASC. Each result includes (windowStart, slotCount)
     * for occupancy-weighted selection.
     *
     * Uses index RL_WNDW_CT_I02X(WNDW_STATUS, WNDW_STRT_TS) for efficient
     * range scan filtered to available windows only.
     */
    fun fetchAvailableWindows(from: Instant, to: Instant, limit: Int): List<Pair<Instant, Int>> {
        return transaction {
            val conn = TransactionManager.current().connection.connection as java.sql.Connection
            conn.prepareStatement(
                """
                SELECT WNDW_STRT_TS, SLOT_CT
                FROM   RL_WNDW_CT
                WHERE  WNDW_STATUS = 'AVAILABLE'
                AND    WNDW_STRT_TS >= ?
                AND    WNDW_STRT_TS < ?
                ORDER BY WNDW_STRT_TS ASC
                FETCH FIRST ? ROWS ONLY
                """.trimIndent()
            ).use { stmt ->
                stmt.setTimestamp(1, Timestamp.from(from))
                stmt.setTimestamp(2, Timestamp.from(to))
                stmt.setInt(3, limit)
                val rs = stmt.executeQuery()
                val results = mutableListOf<Pair<Instant, Int>>()
                while (rs.next()) {
                    results.add(
                        rs.getTimestamp("WNDW_STRT_TS").toInstant() to rs.getInt("SLOT_CT")
                    )
                }
                rs.close()
                results
            }
        }
    }

    /**
     * Absolute-count counter refresh for V7.
     *
     * Statement 1: MERGE sets SLOT_CT to the total slot count for windows that
     * received new inserts between [lastRunTs] and [cutoffTs]. Uses CREAT_TS
     * range in an inner subquery for efficient discovery of recently-active
     * windows, then counts ALL slots per window for an idempotent absolute set.
     * Safe under multi-pod: any pod writes the same correct total count.
     * No WHEN NOT MATCHED — windows are pre-provisioned by
     * WindowPreProvisioningScheduler.
     *
     * Statement 2: Transitions STATUS to 'FULL' for any window that has reached
     * or exceeded [maxSlotsPerWindow].
     */
    fun refreshCountersDelta(lastRunTs: Instant, cutoffTs: Instant, maxSlotsPerWindow: Int) {
        transaction {
            val conn = TransactionManager.current().connection.connection as java.sql.Connection

            // Absolute-count MERGE — set counters to total slot count
            conn.prepareStatement(
                """
                MERGE INTO RL_WNDW_CT b
                USING (
                    SELECT d.WNDW_STRT_TS, COUNT(*) AS total_count
                    FROM RL_EVENT_SLOT_DTL d
                    WHERE d.WNDW_STRT_TS IN (
                        SELECT DISTINCT WNDW_STRT_TS
                        FROM RL_EVENT_SLOT_DTL
                        WHERE CREAT_TS > ?
                          AND CREAT_TS <= ?
                    )
                    GROUP BY d.WNDW_STRT_TS
                ) src
                ON (b.WNDW_STRT_TS = src.WNDW_STRT_TS)
                WHEN MATCHED THEN
                    UPDATE SET b.SLOT_CT = src.total_count
                """.trimIndent()
            ).use { stmt ->
                stmt.setTimestamp(1, Timestamp.from(lastRunTs))
                stmt.setTimestamp(2, Timestamp.from(cutoffTs))
                stmt.executeUpdate()
            }

            // Status transition — mark full windows
            conn.prepareStatement(
                """
                UPDATE RL_WNDW_CT
                SET    WNDW_STATUS = 'FULL'
                WHERE  SLOT_CT >= ?
                AND    WNDW_STATUS = 'AVAILABLE'
                """.trimIndent()
            ).use { stmt ->
                stmt.setInt(1, maxSlotsPerWindow)
                stmt.executeUpdate()
            }
        }
    }

    // ==================== V8 methods ====================

    /**
     * Returns up to [limit] windows with SLOT_CT < [maxSlots] in [from, to),
     * ordered by WNDW_STRT_TS ASC. No status flag check — filters purely on count.
     *
     * Uses index RL_WNDW_CT_I01X(WNDW_STRT_TS, SLOT_CT) for efficient range scan.
     */
    fun fetchWindowsWithAvailableCapacity(
        from: Instant,
        to: Instant,
        maxSlots: Int,
        limit: Int
    ): List<Pair<Instant, Int>> {
        return transaction {
            val conn = TransactionManager.current().connection.connection as java.sql.Connection
            conn.prepareStatement(
                """
                SELECT WNDW_STRT_TS, SLOT_CT
                FROM   RL_WNDW_CT
                WHERE  WNDW_STRT_TS >= ?
                AND    WNDW_STRT_TS < ?
                AND    SLOT_CT < ?
                ORDER BY WNDW_STRT_TS ASC
                FETCH FIRST ? ROWS ONLY
                """.trimIndent()
            ).use { stmt ->
                stmt.setTimestamp(1, Timestamp.from(from))
                stmt.setTimestamp(2, Timestamp.from(to))
                stmt.setInt(3, maxSlots)
                stmt.setInt(4, limit)
                val rs = stmt.executeQuery()
                val results = mutableListOf<Pair<Instant, Int>>()
                while (rs.next()) {
                    results.add(
                        rs.getTimestamp("WNDW_STRT_TS").toInstant() to rs.getInt("SLOT_CT")
                    )
                }
                rs.close()
                results
            }
        }
    }

    // ==================== Legacy methods ====================

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
