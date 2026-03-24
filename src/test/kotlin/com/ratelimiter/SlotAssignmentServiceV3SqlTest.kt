package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV3Sql
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tests for V3 PL/SQL slot assignment.
 * Test profile: windowSize=4s, maxPerWindow=2.
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceV3SqlTest {

    @Inject
    lateinit var service: SlotAssignmentServiceV3Sql

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
        }
    }

    @Test
    fun `assigns first window when capacity available`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val slot = service.assignSlot("evt-1", requestedTime)

        assertEquals("evt-1", slot.eventId)
        assertFalse(slot.scheduledTime.isBefore(requestedTime))
        assertTrue(slot.scheduledTime.isBefore(requestedTime.plusSeconds(4)))
        assertTrue(slot.delay < Duration.ofSeconds(4))
    }

    @Test
    fun `fills multiple windows sequentially`() {
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
    fun `returns existing slot for duplicate eventId`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val first = service.assignSlot("evt-dup", requestedTime)
        val second = service.assignSlot("evt-dup", requestedTime)

        assertEquals(first.eventId, second.eventId)
        assertEquals(first.scheduledTime, second.scheduledTime)
        assertEquals(first.delay, second.delay)

        val count = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-dup" }
                .count()
        }
        assertEquals(1L, count)
    }

    @Test
    fun `concurrent duplicate eventIds all return same slot`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val threadCount = 10

        val results = ConcurrentLinkedQueue<AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(threadCount) {
            executor.submit {
                try {
                    results.add(service.assignSlot("evt-same", requestedTime))
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS))
        executor.shutdown()
        assertTrue(errors.isEmpty())
        assertEquals(threadCount, results.size)
        assertEquals(1, results.map { it.scheduledTime }.distinct().size)
    }

    @Test
    fun `skips full windows to next available`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        service.assignSlot("evt-sk1", requestedTime)
        service.assignSlot("evt-sk2", requestedTime)

        val third = service.assignSlot("evt-sk3", requestedTime)
        assertFalse(third.scheduledTime.isBefore(requestedTime.plusSeconds(4)))
        assertTrue(third.delay >= Duration.ofSeconds(4))
    }

    @Test
    fun `window counter matches actual slot count`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        service.assignSlot("evt-cnt1", requestedTime)
        service.assignSlot("evt-cnt2", requestedTime)

        val counterValue = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq requestedTime }
                .firstOrNull()
                ?.get(WindowCounterTable.slotCount)
        }
        assertEquals(2, counterValue)
    }

    @Test
    fun `concurrent assignments respect max_per_window`() {
        val totalEvents = 100
        val threadCount = 50
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-c$i"] = service.assignSlot("evt-c$i", requestedTime)
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS))
        executor.shutdown()
        assertTrue(errors.isEmpty())
        assertEquals(totalEvents, results.size)

        val slotsByWindow = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
        }
        for ((windowStart, slots) in slotsByWindow) {
            assertTrue(
                slots.size <= 2,
                "Window $windowStart should have at most 2 slots but has ${slots.size}"
            )
        }
    }

    @Test
    fun `no deadlocks under sustained load`() {
        val totalEvents = 200
        val threadCount = 50
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val successCount = AtomicInteger(0)
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    service.assignSlot("evt-dl$i", requestedTime)
                    successCount.incrementAndGet()
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue(latch.await(120, TimeUnit.SECONDS))
        executor.shutdown()
        assertTrue(errors.isEmpty())
        assertEquals(totalEvents, successCount.get())
    }
}
