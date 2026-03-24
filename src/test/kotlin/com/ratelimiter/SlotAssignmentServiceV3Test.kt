package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV3
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
 * Tests for V3 slot assignment with pre-provisioned windows and static config.
 * Test profile: windowSize=4s, maxPerWindow=2.
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceV3Test {

    @Inject
    lateinit var service: SlotAssignmentServiceV3

    @Inject
    lateinit var eventSlotRepository: EventSlotRepository

    @Inject
    lateinit var windowSlotCounterRepository: WindowSlotCounterRepository

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
        }
    }

    private fun preProvisionWindows(from: Instant, windowCount: Long) {
        val windowSize = Duration.ofSeconds(4)
        val windows = (0 until windowCount).map { i ->
            from.plus(windowSize.multipliedBy(i))
        }
        transaction {
            with(windowSlotCounterRepository) { batchInsertWindows(windows) }
        }
    }

    // ==================== Basic assignment ====================

    @Test
    fun `assigns first window when capacity available`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)

        val slot = service.assignSlot("evt-1", requestedTime)

        assertEquals("evt-1", slot.eventId)
        assertFalse(slot.scheduledTime.isBefore(requestedTime))
        assertTrue(slot.scheduledTime.isBefore(requestedTime.plusSeconds(4)))
        assertTrue(slot.delay < Duration.ofSeconds(4))
    }

    @Test
    fun `fills multiple windows sequentially`() {
        // maxPerWindow=2, so 6 events should fill 3 windows
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)

        (1..6).forEach { i ->
            service.assignSlot("evt-multi-$i", requestedTime)
        }

        val windowStarts = transaction {
            RateLimitEventSlotTable.selectAll()
                .map { it[RateLimitEventSlotTable.windowStart] }
                .distinct().sorted()
        }
        assertEquals(3, windowStarts.size)
        assertEquals(Instant.parse("2025-06-01T12:00:00Z"), windowStarts[0])
        assertEquals(Instant.parse("2025-06-01T12:00:04Z"), windowStarts[1])
        assertEquals(Instant.parse("2025-06-01T12:00:08Z"), windowStarts[2])
    }

    @Test
    fun `delay reflects how far event was pushed from requested time`() {
        // maxPerWindow=2, fill first window with 2 events, 3rd goes to next
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)

        val first = service.assignSlot("evt-d1", requestedTime)
        assertTrue(first.delay < Duration.ofSeconds(4))

        service.assignSlot("evt-d2", requestedTime)

        val third = service.assignSlot("evt-d3", requestedTime)
        assertTrue(third.delay >= Duration.ofSeconds(4))
    }

    // ==================== Idempotency ====================

    @Test
    fun `returns existing slot for duplicate eventId`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)

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
        preProvisionWindows(requestedTime, 200)
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

    // ==================== Jitter bounds ====================

    @Test
    fun `jitter stays within window bounds`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)
        val windowEnd = requestedTime.plusSeconds(4)

        // maxPerWindow=2, both slots should be in the first window
        val slots = (1..2).map { i ->
            service.assignSlot("evt-j$i", requestedTime)
        }

        for (slot in slots) {
            assertFalse(slot.scheduledTime.isBefore(requestedTime))
            assertTrue(slot.scheduledTime.isBefore(windowEnd))
        }
    }

    // ==================== Window filling and skip ====================

    @Test
    fun `skips full windows to next available`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)

        // Fill first window (maxPerWindow=2)
        service.assignSlot("evt-sk1", requestedTime)
        service.assignSlot("evt-sk2", requestedTime)

        val third = service.assignSlot("evt-sk3", requestedTime)
        assertFalse(third.scheduledTime.isBefore(requestedTime.plusSeconds(4)))
        assertTrue(third.delay >= Duration.ofSeconds(4))
    }

    @Test
    fun `many windows can be filled sequentially`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)

        // maxPerWindow=2, so 100 events fill 50 windows
        val slots = (1..100).map { i ->
            service.assignSlot("evt-mw$i", requestedTime)
        }

        assertEquals(100, slots.size)
        val maxScheduledTime = slots.maxOf { it.scheduledTime }
        // 50 windows * 4s = 200s range, last window starts at 49*4=196s
        assertFalse(maxScheduledTime.isBefore(requestedTime.plusSeconds(49 * 4L)))
        assertTrue(maxScheduledTime.isBefore(requestedTime.plusSeconds(50 * 4L)))
    }

    // ==================== Exhaustion ====================

    @Test
    fun `throws SlotAssignmentException when all pre-provisioned windows exhausted`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        // Pre-provision only 5 windows → max 10 events (5 * 2)
        preProvisionWindows(requestedTime, 5)

        (1..10).forEach { i ->
            service.assignSlot("evt-ex$i", requestedTime)
        }

        assertThrows(SlotAssignmentException::class.java) {
            service.assignSlot("evt-ex11", requestedTime)
        }
    }

    @Test
    fun `throws SlotAssignmentException when no windows pre-provisioned`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        assertThrows(SlotAssignmentException::class.java) {
            service.assignSlot("evt-np1", requestedTime)
        }
    }

    // ==================== Counter consistency ====================

    @Test
    fun `window counter matches actual slot count`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)

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
    fun `full window counter matches max_per_window`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 200)

        service.assignSlot("evt-fc1", requestedTime)
        service.assignSlot("evt-fc2", requestedTime)

        val slotCount = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq requestedTime }
                .firstOrNull()
                ?.get(WindowCounterTable.slotCount)
        }
        assertEquals(2, slotCount)

        // 3rd event goes to next window
        val third = service.assignSlot("evt-fc3", requestedTime)
        assertFalse(third.scheduledTime.isBefore(requestedTime.plusSeconds(4)))
    }

    // ==================== Isolation ====================

    @Test
    fun `far-future event does not corrupt near-term search`() {
        val farFuture = Instant.parse("2026-06-01T12:00:00Z")
        preProvisionWindows(farFuture, 200)

        val nearTerm = Instant.parse("2025-07-01T12:00:00Z")
        preProvisionWindows(nearTerm, 200)

        val farSlot = service.assignSlot("evt-far", farFuture)
        assertFalse(farSlot.scheduledTime.isBefore(farFuture))

        val nearSlot = service.assignSlot("evt-near", nearTerm)
        assertFalse(nearSlot.scheduledTime.isBefore(nearTerm))
        assertTrue(nearSlot.scheduledTime.isBefore(nearTerm.plusSeconds(4)))
        assertTrue(nearSlot.delay < Duration.ofSeconds(4))
    }

    // ==================== Concurrency ====================

    @Test
    fun `concurrent assignments respect max_per_window`() {
        val totalEvents = 100
        val threadCount = 50
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 500)

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

        // maxPerWindow=2
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
        preProvisionWindows(requestedTime, 500)

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

    @Test
    fun `counter stays consistent under contention`() {
        val totalEvents = 100
        val threadCount = 30
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 500)

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-cc$i"] = service.assignSlot("evt-cc$i", requestedTime)
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

        val slotCountsByWindow = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { it.value.size }
        }

        val counterValues = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }

        for ((windowStart, actualCount) in slotCountsByWindow) {
            assertEquals(
                actualCount, counterValues[windowStart],
                "Counter for $windowStart should match actual slot count $actualCount"
            )
        }

        assertEquals(totalEvents, slotCountsByWindow.values.sum())
    }
}
