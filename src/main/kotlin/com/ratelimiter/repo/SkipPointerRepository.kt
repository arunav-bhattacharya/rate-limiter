package com.ratelimiter.repo

import com.ratelimiter.db.SkipPointerTable
import com.ratelimiter.db.isDuplicateKeyViolation
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Instant

/**
 * Append-only skip pointer repository.
 *
 * Each exhausted chunk produces an INSERT. Reads use ORDER BY DESC LIMIT 1
 * to find the furthest known-exhausted boundary. Zero write contention
 * across 20 pods — concurrent INSERTs never block each other.
 *
 * Mirrors the RL_WNDW_FRONTIER_TRK pattern (append-only, composite PK).
 */
@ApplicationScoped
class SkipPointerRepository {

    /**
     * Read the furthest skip pointer for a given requestedTime.
     * Returns the highest SKIP_TO_TS, or null if no pointer exists.
     * Uses ORDER BY DESC FETCH FIRST 1 ROW — index backward scan on
     * composite PK, stops after one row.
     */
    fun fetchSkipTo(requestedTime: Instant): Instant? {
        return transaction {
            SkipPointerTable
                .selectAll()
                .where { SkipPointerTable.requestedTime eq requestedTime }
                .orderBy(SkipPointerTable.skipTo, SortOrder.DESC)
                .limit(1)
                .firstOrNull()
                ?.get(SkipPointerTable.skipTo)
        }
    }

    /**
     * Record a new skip-to boundary for a given requestedTime.
     * Append-only: inserts a new row. Duplicate (requestedTime, skipTo)
     * pairs are silently caught by the composite PK constraint.
     *
     * Monotonic by construction: fetchSkipTo returns MAX(SKIP_TO_TS),
     * so lower values are automatically ignored without any UPDATE logic.
     */
    fun advanceSkipTo(requestedTime: Instant, newSkipTo: Instant) {
        transaction {
            try {
                SkipPointerTable.insert {
                    it[SkipPointerTable.requestedTime] = requestedTime
                    it[skipTo] = newSkipTo
                    it[createdAt] = Instant.now()
                }
            } catch (e: ExposedSQLException) {
                if (!e.isDuplicateKeyViolation()) throw e
                // Exact same (requestedTime, skipTo) already exists — no-op
            }
        }
    }
}
