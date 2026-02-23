package com.ratelimiter.repo

import com.ratelimiter.db.WindowCounterTable
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.IntegerColumnType
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.javatime.JavaInstantColumnType
import org.jetbrains.exposed.sql.statements.StatementType
import org.jetbrains.exposed.sql.update
import java.time.Instant

@ApplicationScoped
class WindowSlotCounterRepository {

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
                    FETCH FIRST 1 ROW ONLY
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

    fun Transaction.batchInsertWindows(windows: List<Instant>) {

        WindowCounterTable.batchInsert(windows, true, false) { window ->
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
