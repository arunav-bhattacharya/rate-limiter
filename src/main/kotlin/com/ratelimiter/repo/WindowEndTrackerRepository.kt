package com.ratelimiter.repo

import com.ratelimiter.db.WindowEndTrackerTable
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.max
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Instant

@ApplicationScoped
class WindowEndTrackerRepository {

    /**
     * Standalone convenience — reads the provisioning frontier outside a transaction.
     * Used by V2 for backward compatibility.
     */
    fun fetchWindowEnd(requestedTime: Instant): Instant? {
        return transaction {
            fetchMaxWindowEnd(requestedTime)
        }
    }

    /**
     * Reads the provisioning frontier for a given alignedStart.
     * Returns the MAX(window_end) across all frontier rows, or null if none exist.
     * Append-only table — no UPDATE contention.
     */
    fun Transaction.fetchMaxWindowEnd(requestedTime: Instant): Instant? {
        return WindowEndTrackerTable
            .select(WindowEndTrackerTable.windowEnd.max())
            .where { WindowEndTrackerTable.requestedTime eq requestedTime }
            .firstOrNull()
            ?.get(WindowEndTrackerTable.windowEnd.max())
    }

    /**
     * Appends a new frontier row. Catches duplicate key silently —
     * concurrent threads inserting the same (requestedTime, windowEnd)
     * pair are harmless no-ops.
     */
    fun Transaction.insertWindowEnd(requestedTime: Instant, windowEnd: Instant) {
        try {
            WindowEndTrackerTable.insert {
                it[WindowEndTrackerTable.requestedTime] = requestedTime
                it[WindowEndTrackerTable.windowEnd] = windowEnd
            }
        } catch (_: ExposedSQLException) {
            // Duplicate key — another thread already inserted this exact frontier row
        }
    }
}
