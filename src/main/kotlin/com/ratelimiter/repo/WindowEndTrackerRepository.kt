package com.ratelimiter.repo

import com.ratelimiter.db.WindowEndTrackerTable
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import java.time.Instant

@ApplicationScoped
class WindowEndTrackerRepository {

    fun fetchWindowEnd(requestedTime: Instant): Instant? {
        return transaction {
            WindowEndTrackerTable
                .selectAll()
                .where { WindowEndTrackerTable.requestedTime eq requestedTime }
                .firstOrNull()
                ?.get(WindowEndTrackerTable.windowEnd)
        }
    }

    fun Transaction.insertWindowEnd(requestedTime: Instant, windowEnd: Instant) {
        try {
            WindowEndTrackerTable.insert {
                it[WindowEndTrackerTable.requestedTime] = requestedTime
                it[WindowEndTrackerTable.windowEnd] = windowEnd
            }
        } catch (_: ExposedSQLException) {
            // Duplicate key — another thread already inserted, ignore
        }
    }

    fun Transaction.updateWindowEnd(
        requestedTime: Instant,
        currentWindowEnd: Instant,
        newWindowEnd: Instant
    ) {
        WindowEndTrackerTable.update({
            (WindowEndTrackerTable.requestedTime eq requestedTime) and
                    (WindowEndTrackerTable.windowEnd eq currentWindowEnd)
        }) {
            it[WindowEndTrackerTable.windowEnd] = newWindowEnd
        }
    }
}
