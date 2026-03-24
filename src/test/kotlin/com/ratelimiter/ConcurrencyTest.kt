package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.slot.AssignedSlot
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
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Concurrency tests for V1/V2 PL/SQL slot assignment.
 * Test profile: windowSize=4s, maxPerWindow=2.
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class ConcurrencyTest {

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
    fun `concurrent assignments respect max_per_window`() {
        // maxPerWindow=2 from test profile
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
                    results["evt-conc-$i"] = service.assignSlot("evt-conc-$i", requestedTime)
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

        val dbCounters = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }
        for ((windowStart, slots) in slotsByWindow) {
            assertEquals(
                slots.size, dbCounters[windowStart],
                "DB counter for window $windowStart should match actual slot count"
            )
        }
    }

    @Test
    fun `idempotent concurrent calls return same slot`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val eventId = "evt-idem-concurrent"
        val threadCount = 10

        val results = ConcurrentLinkedQueue<AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(threadCount) {
            executor.submit {
                try {
                    results.add(service.assignSlot(eventId, requestedTime))
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

        val dbCount = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq eventId }
                .count()
        }
        assertEquals(1L, dbCount)
    }

    @Test
    fun `counter stays consistent under contention`() {
        val totalEvents = 100
        val threadCount = 30
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-cnt-conc-$i"] = service.assignSlot("evt-cnt-conc-$i", requestedTime)
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
                "Counter for window $windowStart should match actual slot count $actualCount"
            )
        }
        assertEquals(totalEvents, slotCountsByWindow.values.sum())
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
                    service.assignSlot("evt-dl-$i", requestedTime)
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
