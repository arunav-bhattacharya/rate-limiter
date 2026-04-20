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
 * Loads and caches the active [RateLimitConfig] per name. 5s TTL keeps
 * config changes propagating quickly without hitting the DB on every request.
 */
@ApplicationScoped
class RateLimitConfigRepository {

    private data class CachedEntry(val config: RateLimitConfig, val loadedAt: Instant)

    private val cache = ConcurrentHashMap<String, CachedEntry>()
    private val ttl = Duration.ofSeconds(5)

    fun loadActiveConfig(configName: String): RateLimitConfig? {
        val cached = cache[configName]
        if (cached != null && Duration.between(cached.loadedAt, Instant.now()) < ttl) {
            return cached.config
        }
        return transaction {
            RateLimitConfigTable.selectAll()
                .where { (RateLimitConfigTable.configName eq configName) and (RateLimitConfigTable.isActive eq 1) }
                .orderBy(RateLimitConfigTable.effectiveFrom, SortOrder.DESC)
                .limit(1)
                .firstOrNull()
                ?.toModel()
                ?.also { cache[configName] = CachedEntry(it, Instant.now()) }
        }
    }

    fun createConfig(
        configName: String,
        maxPerWindow: Int,
        windowSize: Duration,
        effectiveFrom: Instant = Instant.now()
    ): RateLimitConfig = transaction {
        RateLimitConfigTable.update(
            { (RateLimitConfigTable.configName eq configName) and (RateLimitConfigTable.isActive eq 1) }
        ) { it[isActive] = 0 }

        val newId = UUID.randomUUID().toString()
        val now = Instant.now()
        RateLimitConfigTable.insert {
            it[configId] = newId
            it[RateLimitConfigTable.configName] = configName
            it[RateLimitConfigTable.maxPerWindow] = maxPerWindow
            it[windowSizeIso] = windowSize.toString()
            it[RateLimitConfigTable.effectiveFrom] = effectiveFrom
            it[isActive] = 1
            it[createdAt] = now
        }

        evictCache(configName)
        RateLimitConfig(
            configId = newId,
            configName = configName,
            maxPerWindow = maxPerWindow,
            windowSize = windowSize,
            effectiveFrom = effectiveFrom,
            isActive = true,
            createdAt = now
        )
    }

    fun evictCache() = cache.clear()

    fun evictCache(configName: String) {
        cache.remove(configName)
    }

    fun isCached(configName: String): Boolean = cache.containsKey(configName)

    private fun ResultRow.toModel() = RateLimitConfig(
        configId = this[RateLimitConfigTable.configId],
        configName = this[RateLimitConfigTable.configName],
        maxPerWindow = this[RateLimitConfigTable.maxPerWindow],
        windowSize = Duration.parse(this[RateLimitConfigTable.windowSizeIso]),
        effectiveFrom = this[RateLimitConfigTable.effectiveFrom],
        isActive = this[RateLimitConfigTable.isActive] == 1,
        createdAt = this[RateLimitConfigTable.createdAt],
    )
}
