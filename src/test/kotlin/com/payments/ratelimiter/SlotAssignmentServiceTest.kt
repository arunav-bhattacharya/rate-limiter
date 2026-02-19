package com.payments.ratelimiter

import com.payments.ratelimiter.config.RateLimitConfigRepository
import com.payments.ratelimiter.db.ScheduledEventSlotTable
import com.payments.ratelimiter.db.WindowCounterTable
import com.payments.ratelimiter.slot.SlotAssignmentException
import com.payments.ratelimiter.slot.SlotAssignmentService
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
            ScheduledEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
        }
        configRepository.evictCache()
    }

    @Test
    fun `assignSlot assigns first window when capacity available`() {
        configRepository.createConfig("test-basic", 100, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot = service.assignSlot("evt-basic-1", "test-basic", requestedTime)

        assertThat(slot.eventId).isEqualTo("evt-basic-1")
        assertThat(slot.windowStart).isEqualTo(Instant.parse("2025-06-01T12:00:00Z"))
        assertThat(slot.scheduledTime).isAfterOrEqualTo(slot.windowStart)
        assertThat(slot.scheduledTime).isBefore(slot.windowStart.plusSeconds(4))
        assertThat(slot.slotIndex).isEqualTo(0)
    }

    @Test
    fun `assignSlot returns existing slot for duplicate eventId (idempotency)`() {
        configRepository.createConfig("test-idempotent", 100, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val first = service.assignSlot("evt-idem-1", "test-idempotent", requestedTime)
        val second = service.assignSlot("evt-idem-1", "test-idempotent", requestedTime)

        assertThat(second.slotId).isEqualTo(first.slotId)
        assertThat(second.scheduledTime).isEqualTo(first.scheduledTime)
        assertThat(second.windowStart).isEqualTo(first.windowStart)
        assertThat(second.slotIndex).isEqualTo(first.slotIndex)

        // Verify only one row exists in the DB
        val count = transaction {
            ScheduledEventSlotTable.selectAll()
                .where { ScheduledEventSlotTable.eventId eq "evt-idem-1" }
                .count()
        }
        assertThat(count).isEqualTo(1)
    }

    @Test
    fun `assignSlot skips full windows`() {
        configRepository.createConfig("test-skip", 2, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill window 0 (2 slots max)
        service.assignSlot("evt-skip-1", "test-skip", requestedTime)
        service.assignSlot("evt-skip-2", "test-skip", requestedTime)

        // Third event should go to window 1
        val third = service.assignSlot("evt-skip-3", "test-skip", requestedTime)
        assertThat(third.windowStart).isEqualTo(Instant.parse("2025-06-01T12:00:04Z"))
    }

    @Test
    fun `assignSlot fills multiple windows sequentially`() {
        configRepository.createConfig("test-multi", 3, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slots = (1..9).map { i ->
            service.assignSlot("evt-multi-$i", "test-multi", requestedTime)
        }

        // Should span 3 windows: 0, 4s, 8s
        val windowStarts = slots.map { it.windowStart }.distinct().sorted()
        assertThat(windowStarts).hasSize(3)
        assertThat(windowStarts[0]).isEqualTo(Instant.parse("2025-06-01T12:00:00Z"))
        assertThat(windowStarts[1]).isEqualTo(Instant.parse("2025-06-01T12:00:04Z"))
        assertThat(windowStarts[2]).isEqualTo(Instant.parse("2025-06-01T12:00:08Z"))
    }

    @Test
    fun `jitter is within window bounds`() {
        configRepository.createConfig("test-jitter", 200, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val windowStart = Instant.parse("2025-06-01T12:00:00Z")
        val windowEnd = windowStart.plusSeconds(4)

        val slots = (1..100).map { i ->
            service.assignSlot("evt-jitter-$i", "test-jitter", requestedTime)
        }

        // All scheduled times must be within [windowStart, windowStart + 4s)
        for (slot in slots) {
            assertThat(slot.scheduledTime)
                .isAfterOrEqualTo(windowStart)
                .isBefore(windowEnd)
        }
    }

    @Test
    fun `slot indices increment correctly within a window`() {
        configRepository.createConfig("test-index", 10, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slots = (1..5).map { i ->
            service.assignSlot("evt-idx-$i", "test-index", requestedTime)
        }

        val indices = slots.map { it.slotIndex }
        assertThat(indices).containsExactly(0, 1, 2, 3, 4)
    }

    @Test
    fun `window counter matches actual slot count`() {
        configRepository.createConfig("test-counter", 100, 4)
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
        }.isInstanceOf(com.payments.ratelimiter.slot.ConfigLoadException::class.java)
            .hasMessageContaining("non-existent")
    }

    @Test
    fun `dynamic lookahead grows with assigned windows`() {
        // Use a tiny max_per_window to force multiple windows
        configRepository.createConfig("test-lookahead", 1, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Assign 50 events — should fill 50 windows
        val slots = (1..50).map { i ->
            service.assignSlot("evt-la-$i", "test-lookahead", requestedTime)
        }

        assertThat(slots).hasSize(50)
        val maxWindow = slots.maxOf { it.windowStart }
        // With 50 windows at 4s each, the max window should be 196s ahead
        assertThat(maxWindow).isEqualTo(Instant.parse("2025-06-01T12:00:00Z").plusSeconds(49 * 4L))
    }
}
