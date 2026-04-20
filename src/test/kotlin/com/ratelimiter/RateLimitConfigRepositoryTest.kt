package com.ratelimiter

import com.ratelimiter.repo.RateLimitConfigRepository
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
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
            windowSize = Duration.ofSeconds(4)
        )

        assertTrue(config.configId.isNotBlank())
        assertEquals("test-create", config.configName)
        assertEquals(200, config.maxPerWindow)
        assertEquals(Duration.ofSeconds(4), config.windowSize)
        assertTrue(config.isActive)

        val loaded = repository.loadActiveConfig("test-create")
        assertNotNull(loaded)
        assertEquals(config.configId, loaded?.configId)
        assertEquals(200, loaded?.maxPerWindow)
    }

    @Test
    fun `loadActiveConfig returns null for unknown name`() {
        val result = repository.loadActiveConfig("non-existent-config-${System.nanoTime()}")
        assertNull(result)
    }

    @Test
    fun `createConfig deactivates previous active config`() {
        val first = repository.createConfig(
            configName = "test-deactivate",
            maxPerWindow = 100,
            windowSize = Duration.ofSeconds(4)
        )

        val second = repository.createConfig(
            configName = "test-deactivate",
            maxPerWindow = 200,
            windowSize = Duration.ofSeconds(4)
        )

        val loaded = repository.loadActiveConfig("test-deactivate")
        assertNotNull(loaded)
        assertEquals(second.configId, loaded?.configId)
        assertEquals(200, loaded?.maxPerWindow)
        assertNotEquals(first.configId, loaded?.configId)
    }

    @Test
    fun `loadActiveConfig returns most recent by effectiveFrom`() {
        val earlier = Instant.parse("2025-01-01T00:00:00Z")
        val later = Instant.parse("2025-06-01T00:00:00Z")

        // Deactivate any existing for this name first
        repository.createConfig("test-order", 50, Duration.ofSeconds(2), earlier)
        val newest = repository.createConfig("test-order", 100, Duration.ofSeconds(4), later)

        val loaded = repository.loadActiveConfig("test-order")
        assertNotNull(loaded)
        assertEquals(newest.configId, loaded?.configId)
    }

    @Test
    fun `cache returns same config within TTL`() {
        repository.createConfig("test-cache", 100, Duration.ofSeconds(4))

        val first = repository.loadActiveConfig("test-cache")
        assertNotNull(first)

        // Second call should come from cache
        val second = repository.loadActiveConfig("test-cache")
        assertNotNull(second)
        assertEquals(first?.configId, second?.configId)
        assertTrue(repository.isCached("test-cache"))
    }

    @Test
    fun `evictCache forces DB reload`() {
        repository.createConfig("test-evict", 100, Duration.ofSeconds(4))
        repository.loadActiveConfig("test-evict")
        assertTrue(repository.isCached("test-evict"))

        repository.evictCache()
        assertFalse(repository.isCached("test-evict"))

        // Next call should still succeed (loads from DB)
        val reloaded = repository.loadActiveConfig("test-evict")
        assertNotNull(reloaded)
    }
}
