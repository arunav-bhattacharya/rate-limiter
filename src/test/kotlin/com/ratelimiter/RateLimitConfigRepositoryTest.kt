package com.ratelimiter

import com.ratelimiter.config.RateLimitConfigRepository
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant

@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class RateLimitConfigRepositoryTest {

    @Inject
    lateinit var repository: RateLimitConfigRepository

    @BeforeEach
    fun setup() {
        repository.evictCache()
    }

    @Test
    fun `createConfig creates retrievable config`() {
        val config = repository.createConfig(
            configName = "test-create",
            maxPerWindow = 200,
            windowSizeSecs = 4
        )

        assertThat(config.id).isGreaterThan(0)
        assertThat(config.configName).isEqualTo("test-create")
        assertThat(config.maxPerWindow).isEqualTo(200)
        assertThat(config.windowSizeSecs).isEqualTo(4)
        assertThat(config.isActive).isTrue()

        val loaded = repository.loadActiveConfig("test-create")
        assertThat(loaded).isNotNull
        assertThat(loaded?.id).isEqualTo(config.id)
        assertThat(loaded?.maxPerWindow).isEqualTo(200)
    }

    @Test
    fun `loadActiveConfig returns null for unknown name`() {
        val result = repository.loadActiveConfig("non-existent-config-${System.nanoTime()}")
        assertThat(result).isNull()
    }

    @Test
    fun `createConfig deactivates previous active config`() {
        val first = repository.createConfig(
            configName = "test-deactivate",
            maxPerWindow = 100,
            windowSizeSecs = 4
        )

        val second = repository.createConfig(
            configName = "test-deactivate",
            maxPerWindow = 200,
            windowSizeSecs = 4
        )

        val loaded = repository.loadActiveConfig("test-deactivate")
        assertThat(loaded).isNotNull
        assertThat(loaded?.id).isEqualTo(second.id)
        assertThat(loaded?.maxPerWindow).isEqualTo(200)
        assertThat(loaded?.id).isNotEqualTo(first.id)
    }

    @Test
    fun `loadActiveConfig returns most recent by effectiveFrom`() {
        val earlier = Instant.parse("2025-01-01T00:00:00Z")
        val later = Instant.parse("2025-06-01T00:00:00Z")

        // Deactivate any existing for this name first
        repository.createConfig("test-order", 50, 2, earlier)
        val newest = repository.createConfig("test-order", 100, 4, later)

        val loaded = repository.loadActiveConfig("test-order")
        assertThat(loaded).isNotNull
        assertThat(loaded?.id).isEqualTo(newest.id)
    }

    @Test
    fun `cache returns same config within TTL`() {
        repository.createConfig("test-cache", 100, 4)

        val first = repository.loadActiveConfig("test-cache")
        assertThat(first).isNotNull

        // Second call should come from cache
        val second = repository.loadActiveConfig("test-cache")
        assertThat(second).isNotNull
        assertThat(second?.id).isEqualTo(first?.id)
        assertThat(repository.isCached("test-cache")).isTrue()
    }

    @Test
    fun `evictCache forces DB reload`() {
        repository.createConfig("test-evict", 100, 4)
        repository.loadActiveConfig("test-evict")
        assertThat(repository.isCached("test-evict")).isTrue()

        repository.evictCache()
        assertThat(repository.isCached("test-evict")).isFalse()

        // Next call should still succeed (loads from DB)
        val reloaded = repository.loadActiveConfig("test-evict")
        assertThat(reloaded).isNotNull
    }
}
