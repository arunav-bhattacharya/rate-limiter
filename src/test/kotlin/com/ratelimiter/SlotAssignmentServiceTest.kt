package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.slot.SlotAssignmentService
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/**
 * Tests for V1/V2 PL/SQL slot assignment.
 * Test profile: windowSize=4s, maxPerWindow=2.
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceTest {

    @Inject
    lateinit var service: SlotAssignmentService

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
        }
    }

    @Test
    fun `assignSlot assigns first window when capacity available`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val slot = service.assignSlot("evt-basic-1", requestedTime)

        assertEquals("evt-basic-1", slot.eventId)
        assertFalse(slot.scheduledTime.isBefore(requestedTime))
        assertTrue(slot.scheduledTime.isBefore(requestedTime.plusSeconds(4)))
        assertTrue(slot.delay < Duration.ofSeconds(4))
    }

    @Test
    fun `assignSlot returns existing slot for duplicate eventId (idempotency)`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val first = service.assignSlot("evt-idem-1", requestedTime)
        val second = service.assignSlot("evt-idem-1", requestedTime)

        assertEquals(first.eventId, second.eventId)
        assertEquals(first.scheduledTime, second.scheduledTime)
        assertEquals(first.delay, second.delay)

        val count = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-idem-1" }
                .count()
        }
        assertEquals(1L, count)
    }

    @Test
    fun `assignSlot skips full windows`() {
        // maxPerWindow=2 in test profile
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        service.assignSlot("evt-skip-1", requestedTime)
        service.assignSlot("evt-skip-2", requestedTime)

        val third = service.assignSlot("evt-skip-3", requestedTime)
        assertFalse(third.scheduledTime.isBefore(requestedTime.plusSeconds(4)))
        assertTrue(third.delay >= Duration.ofSeconds(4))
    }

    @Test
    fun `assignSlot fills multiple windows sequentially`() {
        // maxPerWindow=2, 6 events → 3 windows
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        (1..6).forEach { i ->
            service.assignSlot("evt-multi-$i", requestedTime)
        }

        val windowStarts = transaction {
            RateLimitEventSlotTable.selectAll()
                .map { it[RateLimitEventSlotTable.windowStart] }
                .distinct().sorted()
        }
        assertEquals(3, windowStarts.size)
    }

    @Test
    fun `window counter matches actual slot count`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        service.assignSlot("evt-cnt-1", requestedTime)
        service.assignSlot("evt-cnt-2", requestedTime)

        val counterValue = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq requestedTime }
                .firstOrNull()
                ?.get(WindowCounterTable.slotCount)
        }
        assertEquals(2, counterValue)
    }

    @Test
    fun `many windows can be filled sequentially`() {
        // maxPerWindow=2, 50 events → 25 windows
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slots = (1..50).map { i ->
            service.assignSlot("evt-mw-$i", requestedTime)
        }

        assertEquals(50, slots.size)
        val maxScheduledTime = slots.maxOf { it.scheduledTime }
        assertFalse(maxScheduledTime.isBefore(requestedTime.plusSeconds(24 * 4L)))
        assertTrue(maxScheduledTime.isBefore(requestedTime.plusSeconds(25 * 4L)))
    }

    @Test
    fun `delay reflects how far event was pushed from requested time`() {
        // maxPerWindow=2
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val first = service.assignSlot("evt-delay-1", requestedTime)
        assertTrue(first.delay < Duration.ofSeconds(4))

        service.assignSlot("evt-delay-2", requestedTime)

        val third = service.assignSlot("evt-delay-3", requestedTime)
        assertTrue(third.delay >= Duration.ofSeconds(4))
    }

    @Test
    fun `full window counter matches max_per_window`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        service.assignSlot("evt-fc-1", requestedTime)
        service.assignSlot("evt-fc-2", requestedTime)

        val slotCount = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq requestedTime }
                .firstOrNull()
                ?.get(WindowCounterTable.slotCount)
        }
        assertEquals(2, slotCount)

        val third = service.assignSlot("evt-fc-3", requestedTime)
        assertFalse(third.scheduledTime.isBefore(requestedTime.plusSeconds(4)))
    }
}
