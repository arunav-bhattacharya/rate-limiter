package com.ratelimiter.repo

import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.sql.IntegerColumnType
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.intParam
import org.jetbrains.exposed.sql.javatime.JavaInstantColumnType
import org.jetbrains.exposed.sql.javatime.timestampParam
import org.jetbrains.exposed.sql.statements.StatementType
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
                    FROM   WINDOW_COUNTER
                    WHERE
                           WINDOW_START >= ?
                    AND    WINDOW_START <= ?
                    AND    SLOT_COUNT < ?
                    ORDER BY WINDOW_START ASC
                    FETCH FIRST 1 ROW ONLY
                    FOR UPDATE SKIP LOCKED;
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
}
