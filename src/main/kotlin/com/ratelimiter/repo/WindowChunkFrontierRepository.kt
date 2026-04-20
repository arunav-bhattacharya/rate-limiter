package com.ratelimiter.repo

import com.ratelimiter.db.WindowChunkFrontierTable
import com.ratelimiter.db.isDuplicateKeyViolation
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.max
import org.jetbrains.exposed.sql.select
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * Tracks the furthest provisioned window boundary per requestedTime.
 * Append-only with composite PK (REQ_TS, WNDW_END_TS) — no UPDATE contention.
 *
 * In-memory cache holds the monotonically advancing max for each requestedTime
 * to skip the DB read on the hot path.
 */
@ApplicationScoped
class WindowChunkFrontierRepository {

    private val cache = ConcurrentHashMap<Instant, Instant>()

    /** Cache-only read. Returns null on miss; caller should fall through to a DB read. */
    fun fetchMaxWindowEndCached(requestedTime: Instant): Instant? = cache[requestedTime]

    /** DB read inside the caller's transaction. Populates the cache when a row is found. */
    fun Transaction.fetchMaxWindowEndFromDb(requestedTime: Instant): Instant? {
        val maxExpr = WindowChunkFrontierTable.windowEnd.max()
        val end = WindowChunkFrontierTable
            .select(maxExpr)
            .where { WindowChunkFrontierTable.requestedTime eq requestedTime }
            .firstOrNull()
            ?.get(maxExpr)
        if (end != null) {
            cache.merge(requestedTime, end) { a, b -> if (a.isAfter(b)) a else b }
        }
        return end
    }

    /**
     * Append-only insert. Composite PK absorbs duplicates from concurrent provisioners.
     */
    fun Transaction.insertWindowFrontier(requestedTime: Instant, windowEnd: Instant) {
        try {
            WindowChunkFrontierTable.insert {
                it[WindowChunkFrontierTable.requestedTime] = requestedTime
                it[WindowChunkFrontierTable.windowEnd] = windowEnd
                it[createdAt] = Instant.now()
            }
        } catch (e: ExposedSQLException) {
            if (!e.isDuplicateKeyViolation()) throw e
        }
        cache.merge(requestedTime, windowEnd) { a, b -> if (a.isAfter(b)) a else b }
    }
}
