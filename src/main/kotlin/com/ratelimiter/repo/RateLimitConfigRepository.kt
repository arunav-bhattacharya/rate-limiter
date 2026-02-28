package com.ratelimiter.repo

import com.ratelimiter.config.RateLimitConfig
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
import java.util.UUID
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
        effectiveFrom: Instant = Instant.now()
    ): RateLimitConfig {
        require(!windowSize.isZero && !windowSize.isNegative) { "windowSize must be positive" }

        val now = Instant.now()
        val newConfigId = UUID.randomUUID().toString()
        transaction {
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
                it[configId] = newConfigId
                it[RateLimitConfigTable.configName] = configName
                it[RateLimitConfigTable.maxPerWindow] = maxPerWindow
                it[RateLimitConfigTable.windowSize] = windowSize.toString()
                it[RateLimitConfigTable.effectiveFrom] = effectiveFrom
                it[isActive] = true
                it[createdAt] = now
            }
        }

        cache.remove(configName)

        return RateLimitConfig(
            configId = newConfigId,
            configName = configName,
            maxPerWindow = maxPerWindow,
            windowSize = windowSize,
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
        effectiveFrom = this[RateLimitConfigTable.effectiveFrom],
        isActive = this[RateLimitConfigTable.isActive],
        createdAt = this[RateLimitConfigTable.createdAt]
    )
}
