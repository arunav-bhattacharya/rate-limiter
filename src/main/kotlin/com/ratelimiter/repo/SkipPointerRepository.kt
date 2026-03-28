package com.ratelimiter.repo

import com.ratelimiter.db.SkipPointerTable
import com.ratelimiter.db.isDuplicateKeyViolation
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import java.time.Instant

@ApplicationScoped
class SkipPointerRepository {

    /**
     * Read the skip pointer for a given requestedTime.
     * Returns the first potentially available window, or null if no pointer exists.
     * Own short-lived transaction (~0.1ms PK lookup).
     */
    fun fetchSkipTo(requestedTime: Instant): Instant? {
        return transaction {
            SkipPointerTable
                .selectAll()
                .where { SkipPointerTable.requestedTime eq requestedTime }
                .firstOrNull()
                ?.get(SkipPointerTable.skipTo)
        }
    }

    /**
     * Advance the skip pointer monotonically. Only moves forward (never backwards).
     * Safe across 20 pods: UPDATE with WHERE SKIP_TO_TS < ? ensures highest value wins.
     * INSERT race handled via duplicate-key catch.
     */
    fun advanceSkipTo(requestedTime: Instant, newSkipTo: Instant) {
        transaction {
            val updated = SkipPointerTable.update({
                (SkipPointerTable.requestedTime eq requestedTime) and
                        (SkipPointerTable.skipTo less newSkipTo)
            }) {
                it[skipTo] = newSkipTo
                it[updatedAt] = Instant.now()
            }

            if (updated == 0) {
                try {
                    SkipPointerTable.insert {
                        it[SkipPointerTable.requestedTime] = requestedTime
                        it[skipTo] = newSkipTo
                        it[updatedAt] = Instant.now()
                    }
                } catch (e: ExposedSQLException) {
                    if (!e.isDuplicateKeyViolation()) throw e
                    // Row exists but newSkipTo <= current — no-op (monotonic guarantee)
                }
            }
        }
    }
}
