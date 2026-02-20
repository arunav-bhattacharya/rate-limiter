package com.ratelimiter

import com.ratelimiter.config.RateLimitConfigRepository
import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.slot.ConfigLoadException
import com.ratelimiter.slot.SlotAssignmentService
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceTest {

    @Inject
    lateinit var service: SlotAssignmentService

    @Inject
    lateinit var configRepository: RateLimitConfigRepository

    @BeforeEach
    fun setup() {
        // Clean slate for each test
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
        }
        configRepository.evictCache()
    }

    @Test
    fun `assignSlot assigns first window when capacity available`() {
        configRepository.createConfig("test-basic", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot = service.assignSlot("evt-basic-1", "test-basic", requestedTime)

        assertThat(slot.eventId).isEqualTo("evt-basic-1")
        assertThat(slot.scheduledTime).isAfterOrEqualTo(requestedTime)
        assertThat(slot.scheduledTime).isBefore(requestedTime.plusSeconds(4))
        // Event lands in the requested window, so delay is within the window size
        assertThat(slot.delay).isLessThan(Duration.ofSeconds(4))
    }

    @Test
    fun `assignSlot returns existing slot for duplicate eventId (idempotency)`() {
        configRepository.createConfig("test-idempotent", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val first = service.assignSlot("evt-idem-1", "test-idempotent", requestedTime)
        val second = service.assignSlot("evt-idem-1", "test-idempotent", requestedTime)

        assertThat(second.eventId).isEqualTo(first.eventId)
        assertThat(second.scheduledTime).isEqualTo(first.scheduledTime)
        assertThat(second.delay).isEqualTo(first.delay)

        // Verify only one row exists in the DB
        val count = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-idem-1" }
                .count()
        }
        assertThat(count).isEqualTo(1)
    }

    @Test
    fun `assignSlot skips full windows`() {
        configRepository.createConfig("test-skip", 2, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill window 0 (2 slots max)
        service.assignSlot("evt-skip-1", "test-skip", requestedTime)
        service.assignSlot("evt-skip-2", "test-skip", requestedTime)

        // Third event should go to window 1 (4s later), so delay >= 4s
        val third = service.assignSlot("evt-skip-3", "test-skip", requestedTime)
        assertThat(third.scheduledTime).isAfterOrEqualTo(requestedTime.plusSeconds(4))
        assertThat(third.delay).isGreaterThanOrEqualTo(Duration.ofSeconds(4))
    }

    @Test
    fun `assignSlot fills multiple windows sequentially`() {
        configRepository.createConfig("test-multi", 3, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slots = (1..9).map { i ->
            service.assignSlot("evt-multi-$i", "test-multi", requestedTime)
        }

        // Should span 3 windows: events should have scheduled times across 12s range
        // Verify via DB that 3 distinct windows were used
        val windowStarts = transaction {
            RateLimitEventSlotTable.selectAll().map { it[RateLimitEventSlotTable.windowStart] }.distinct().sorted()
        }
        assertThat(windowStarts).hasSize(3)
        assertThat(windowStarts[0]).isEqualTo(Instant.parse("2025-06-01T12:00:00Z"))
        assertThat(windowStarts[1]).isEqualTo(Instant.parse("2025-06-01T12:00:04Z"))
        assertThat(windowStarts[2]).isEqualTo(Instant.parse("2025-06-01T12:00:08Z"))
    }

    @Test
    fun `jitter is within window bounds`() {
        configRepository.createConfig("test-jitter", 200, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val windowEnd = requestedTime.plusSeconds(4)

        val slots = (1..100).map { i ->
            service.assignSlot("evt-jitter-$i", "test-jitter", requestedTime)
        }

        // All scheduled times must be within [requestedTime, requestedTime + 4s)
        for (slot in slots) {
            assertThat(slot.scheduledTime)
                .isAfterOrEqualTo(requestedTime)
                .isBefore(windowEnd)
        }
    }

    @Test
    fun `window counter matches actual slot count`() {
        configRepository.createConfig("test-counter", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        repeat(10) { i ->
            service.assignSlot("evt-cnt-$i", "test-counter", requestedTime)
        }

        val counterValue = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq Instant.parse("2025-06-01T12:00:00Z") }
                .firstOrNull()
                ?.get(WindowCounterTable.slotCount)
        }

        assertThat(counterValue).isEqualTo(10)
    }

    @Test
    fun `throws ConfigLoadException for unknown config name`() {
        assertThatThrownBy {
            service.assignSlot("evt-bad-config", "non-existent", Instant.now())
        }.isInstanceOf(ConfigLoadException::class.java)
            .hasMessageContaining("non-existent")
    }

    @Test
    fun `dynamic lookahead grows with assigned windows`() {
        // Use a tiny max_per_window to force multiple windows
        configRepository.createConfig("test-lookahead", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Assign 50 events — should fill 50 windows
        val slots = (1..50).map { i ->
            service.assignSlot("evt-la-$i", "test-lookahead", requestedTime)
        }

        assertThat(slots).hasSize(50)
        // Last event should be pushed ~196s ahead (49 windows × 4s + jitter)
        val maxScheduledTime = slots.maxOf { it.scheduledTime }
        assertThat(maxScheduledTime).isAfterOrEqualTo(requestedTime.plusSeconds(49 * 4L))
        assertThat(maxScheduledTime).isBefore(requestedTime.plusSeconds(50 * 4L))
    }

    @Test
    fun `delay reflects how far event was pushed from requested time`() {
        configRepository.createConfig("test-delay", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // First event lands in the requested window — delay < windowSize
        val first = service.assignSlot("evt-delay-1", "test-delay", requestedTime)
        assertThat(first.delay).isLessThan(Duration.ofSeconds(4))

        // Second event is pushed to next window — delay >= 4s
        val second = service.assignSlot("evt-delay-2", "test-delay", requestedTime)
        assertThat(second.delay).isGreaterThanOrEqualTo(Duration.ofSeconds(4))

        // Third event is pushed to third window — delay >= 8s
        val third = service.assignSlot("evt-delay-3", "test-delay", requestedTime)
        assertThat(third.delay).isGreaterThanOrEqualTo(Duration.ofSeconds(8))
    }
}
