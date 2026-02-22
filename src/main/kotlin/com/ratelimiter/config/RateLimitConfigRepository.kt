package com.ratelimiter.config

import com.ratelimiter.db.RateLimitConfigTable
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * Loads, caches, and updates rate limit configurations.
 *
 * Uses a 5-second in-memory cache per config name to minimize DB round-trips
 * on the hot path. Cache entries are evicted on write operations.
 */
@ApplicationScoped
class RateLimitConfigRepository {

    private data class CachedConfig(
        val config: RateLimitConfig,
        val loadedAt: Instant
    )

    private val cache = ConcurrentHashMap<String, CachedConfig>()

    companion object {
        private const val CACHE_TTL_SECONDS = 5L
    }

    /**
     * Load the most recent active config for the given name.
     * Returns from the 5-second cache if fresh; otherwise queries DB.
     *
     * @return the active config, or null if none exists
     */
    fun loadActiveConfig(configName: String): RateLimitConfig? {
        val now = Instant.now()
        val cached = cache[configName]
        if (cached != null && Duration.between(cached.loadedAt, now).seconds < CACHE_TTL_SECONDS) {
            return cached.config
        }

        val config = transaction {
            RateLimitConfigTable
                .selectAll()
                .where {
                    (RateLimitConfigTable.configName eq configName) and
                        (RateLimitConfigTable.isActive eq true)
                }
                .orderBy(RateLimitConfigTable.effectiveFrom, SortOrder.DESC)
                .limit(1)
                .map { it.toRateLimitConfig() }
                .firstOrNull()
        }

        if (config != null) {
            cache[configName] = CachedConfig(config, now)
        }
        return config
    }

    /**
     * Insert a new config version and deactivate all previous active configs
     * for the same name. Returns the newly created config.
     *
     * @param windowSize the time window duration (e.g., Duration.ofSeconds(4))
     */
    fun createConfig(
        configName: String,
        maxPerWindow: Int,
        windowSize: Duration,
        headroomWindows: Int? = null,
        effectiveFrom: Instant = Instant.now()
    ): RateLimitConfig {
        require(maxPerWindow > 0) { "maxPerWindow must be positive" }
        require(!windowSize.isZero && !windowSize.isNegative) { "windowSize must be positive" }
        require(windowSize.toSeconds() > 0) { "windowSize must be at least 1 second" }
        require(windowSize.toMillis() % 1000L == 0L) { "windowSize must be an integer number of seconds" }
        require(headroomWindows == null || headroomWindows > 0) { "headroomWindows must be positive when provided" }

        val now = Instant.now()
        val windowSizeText = windowSize.toString()
        val insertedId = transaction {
            val mismatchedWindowSizeExists = !RateLimitConfigTable
                .selectAll()
                .where {
                    (RateLimitConfigTable.isActive eq true) and
                        (RateLimitConfigTable.windowSize neq windowSizeText)
                }
                .limit(1)
                .empty()

            if (mismatchedWindowSizeExists) {
                throw IllegalArgumentException(
                    "All active configs must share the same windowSize. " +
                        "Create/update rejected for windowSize=$windowSizeText"
                )
            }

            // Deactivate existing active configs for this name
            RateLimitConfigTable.update(
                where = {
                    (RateLimitConfigTable.configName eq configName) and
                        (RateLimitConfigTable.isActive eq true)
                }
            ) {
                it[isActive] = false
            }

            // Insert new config
            RateLimitConfigTable.insert {
                it[RateLimitConfigTable.configName] = configName
                it[RateLimitConfigTable.maxPerWindow] = maxPerWindow
                it[RateLimitConfigTable.windowSize] = windowSizeText
                it[RateLimitConfigTable.headroomWindows] = headroomWindows
                it[RateLimitConfigTable.effectiveFrom] = effectiveFrom
                it[isActive] = true
                it[createdAt] = now
            } get RateLimitConfigTable.configId
        }

        cache.remove(configName)

        return RateLimitConfig(
            configId = insertedId,
            configName = configName,
            maxPerWindow = maxPerWindow,
            windowSize = windowSize,
            headroomWindows = headroomWindows,
            effectiveFrom = effectiveFrom,
            isActive = true,
            createdAt = now
        )
    }

    /** Force-evict all cached entries. Used for immediate config propagation. */
    fun evictCache() {
        cache.clear()
    }

    /** Check if a config name has a cached entry (for testing). */
    fun isCached(configName: String): Boolean {
        val cached = cache[configName] ?: return false
        return Duration.between(cached.loadedAt, Instant.now()).seconds < CACHE_TTL_SECONDS
    }

    private fun ResultRow.toRateLimitConfig(): RateLimitConfig = RateLimitConfig(
        configId = this[RateLimitConfigTable.configId],
        configName = this[RateLimitConfigTable.configName],
        maxPerWindow = this[RateLimitConfigTable.maxPerWindow],
        windowSize = Duration.parse(this[RateLimitConfigTable.windowSize]),
        headroomWindows = this[RateLimitConfigTable.headroomWindows],
        effectiveFrom = this[RateLimitConfigTable.effectiveFrom],
        isActive = this[RateLimitConfigTable.isActive],
        createdAt = this[RateLimitConfigTable.createdAt]
    )
}
