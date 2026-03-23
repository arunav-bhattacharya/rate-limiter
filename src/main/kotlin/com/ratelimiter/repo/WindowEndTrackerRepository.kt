package com.ratelimiter.repo

import com.ratelimiter.db.WindowEndTrackerTable
import com.ratelimiter.db.isDuplicateKeyViolation
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class WindowEndTrackerRepository {

    private data class CachedFrontier(
        val windowEnd: Instant,
        val loadedAt: Instant
    )

    private val frontierCache = ConcurrentHashMap<Instant, CachedFrontier>()

    companion object {
        private const val CACHE_TTL_SECONDS = 5L
    }

    fun evictFrontierCache() = frontierCache.clear()

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
     * Fast-path cache check — no transaction or connection needed.
     * Returns the cached frontier if present and fresh, null otherwise.
     */
    fun fetchMaxWindowEndCached(requestedTime: Instant): Instant? {
        val now = Instant.now()
        val cached = frontierCache[requestedTime]
        if (cached != null && Duration.between(cached.loadedAt, now).seconds < CACHE_TTL_SECONDS) {
            return cached.windowEnd
        }
        return null
    }

    /**
     * Reads the provisioning frontier for a given alignedStart from the database.
     * Updates the JVM cache on hit so subsequent calls can use [fetchMaxWindowEndCached].
     */
    fun Transaction.fetchMaxWindowEndFromDb(requestedTime: Instant): Instant? {
        val maxWindowEnd: Instant? = WindowEndTrackerTable
            .select(WindowEndTrackerTable.windowEnd)
            .where { WindowEndTrackerTable.requestedTime eq requestedTime }
            .orderBy(WindowEndTrackerTable.windowEnd, SortOrder.DESC)
            .limit(1)
            .firstOrNull()
            ?.get(WindowEndTrackerTable.windowEnd)

        if (maxWindowEnd != null) {
            frontierCache[requestedTime] = CachedFrontier(maxWindowEnd, Instant.now())
        }
        return maxWindowEnd
    }

    /**
     * Reads the provisioning frontier for a given alignedStart.
     * Returns the MAX(WNDW_END_TS) across all frontier rows, or null if none exist.
     * Uses a 5-second JVM cache to avoid the MAX() aggregation on the hot path.
     */
    fun Transaction.fetchMaxWindowEnd(requestedTime: Instant): Instant? {
        return fetchMaxWindowEndCached(requestedTime)
            ?: fetchMaxWindowEndFromDb(requestedTime)
    }

    /**
     * Appends a new frontier row. Catches duplicate key silently —
     * concurrent threads inserting the same (REQ_TS, WNDW_END_TS)
     * pair are harmless no-ops.
     *
     * Updates the JVM cache so subsequent reads on this pod skip the DB.
     */
    fun Transaction.insertWindowEnd(requestedTime: Instant, windowEnd: Instant) {
        try {
            WindowEndTrackerTable.insert {
                it[WindowEndTrackerTable.requestedTime] = requestedTime
                it[WindowEndTrackerTable.windowEnd] = windowEnd
                it[createdAt] = Instant.now()
            }
        } catch (e: ExposedSQLException) {
            if (!e.isDuplicateKeyViolation()) throw e
        }
        frontierCache.merge(requestedTime, CachedFrontier(windowEnd, Instant.now())) { old, new ->
            if (new.windowEnd > old.windowEnd) new else old.copy(loadedAt = new.loadedAt)
        }
    }
}
