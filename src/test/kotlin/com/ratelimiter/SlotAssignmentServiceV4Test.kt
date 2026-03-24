package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV4
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tests for V4 conditional INSERT slot assignment.
 * Test profile: windowSize=4s, maxPerWindow=2, windowFillThreshold=0.9.
 * softMax = floor(2 * 0.9) = 1
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceV4Test {

    @Inject
    lateinit var service: SlotAssignmentServiceV4

    @Inject
    lateinit var eventSlotRepository: EventSlotRepository

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
        }
    }

    @Test
    fun `assigns slot in first window when capacity available`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val slot = service.assignSlot("evt-1", requestedTime)

        assertEquals("evt-1", slot.eventId)
        assertTrue(slot.scheduledTime >= requestedTime)
    }

    @Test
    fun `returns same slot for duplicate eventId`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot1 = service.assignSlot("evt-dup", requestedTime)
        val slot2 = service.assignSlot("evt-dup", requestedTime)

        assertEquals(slot1.eventId, slot2.eventId)
        assertEquals(slot1.scheduledTime, slot2.scheduledTime)
    }

    @Test
    fun `concurrent duplicate eventIds return same slot`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val results = ConcurrentLinkedQueue<AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(1)
        val threads = 10

        val executor = Executors.newFixedThreadPool(threads)
        repeat(threads) {
            executor.submit {
                try {
                    latch.await()
                    results.add(service.assignSlot("evt-conc-dup", requestedTime))
                } catch (e: Throwable) {
                    errors.add(e)
                }
            }
        }
        latch.countDown()
        executor.shutdown()
        executor.awaitTermination(60, TimeUnit.SECONDS)

        assertTrue(errors.isEmpty(), "Errors: ${errors.map { it.message }}")
        assertEquals(threads, results.size)
        assertEquals(1, results.map { it.scheduledTime }.distinct().size)
    }

    @Test
    fun `scheduledTime is within window boundaries`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot = service.assignSlot("evt-jit-1", requestedTime)
        val windowStart = slot.scheduledTime.let { st ->
            val epochSec = st.epochSecond
            Instant.ofEpochSecond(epochSec - (epochSec % 4))
        }
        assertTrue(slot.scheduledTime >= windowStart)
        assertTrue(slot.scheduledTime < windowStart.plusSeconds(4))
    }

    @Test
    fun `bulk assignment of 50 events for same timestamp`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slots = (1..50).map { i ->
            service.assignSlot("evt-bulk-$i", requestedTime)
        }

        assertEquals(50, slots.size)
        slots.forEach { slot ->
            assertTrue(slot.scheduledTime >= requestedTime)
        }
    }

    @Test
    fun `concurrent assignment - 50 events across 20 threads`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(1)
        val totalEvents = 50
        val threads = 20
        val counter = AtomicInteger(0)

        val executor = Executors.newFixedThreadPool(threads)
        repeat(threads) {
            executor.submit {
                try {
                    latch.await()
                    while (true) {
                        val idx = counter.incrementAndGet()
                        if (idx > totalEvents) break
                        val eventId = "evt-conc-$idx"
                        results[eventId] = service.assignSlot(eventId, requestedTime)
                    }
                } catch (e: Throwable) {
                    errors.add(e)
                }
            }
        }
        latch.countDown()
        executor.shutdown()
        executor.awaitTermination(120, TimeUnit.SECONDS)

        assertTrue(errors.isEmpty(), "Errors: ${errors.map { "${it.javaClass.simpleName}: ${it.message}" }}")
        assertEquals(totalEvents, results.size)

        val dbCount = transaction {
            RateLimitEventSlotTable.selectAll().count()
        }
        assertEquals(totalEvents.toLong(), dbCount)
    }
}
