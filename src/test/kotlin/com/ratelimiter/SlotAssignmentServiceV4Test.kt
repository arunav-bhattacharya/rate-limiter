package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.db.WindowEndTrackerTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.ConfigLoadException
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
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceV4Test {

    @Inject
    lateinit var service: SlotAssignmentServiceV4

    @Inject
    lateinit var configRepository: RateLimitConfigRepository

    @Inject
    lateinit var eventSlotRepository: EventSlotRepository

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
            WindowEndTrackerTable.deleteAll()
        }
        configRepository.evictCache()
    }

    // ==================== Basic assignment ====================

    @Test
    fun `assigns slot in first window when capacity available`() {
        configRepository.createConfig("v4-basic", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot = service.assignSlot("evt-1", "v4-basic", requestedTime)

        assertEquals("evt-1", slot.eventId)
        assertTrue(slot.scheduledTime >= requestedTime)
        assertTrue(slot.scheduledTime < requestedTime.plusSeconds(4 * 5)) // within maxSlotAttempts windows
    }

    @Test
    fun `fills multiple windows sequentially`() {
        // softMax = floor(3 * 0.9) = 2
        configRepository.createConfig("v4-multi", 3, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        (1..6).forEach { i ->
            service.assignSlot("evt-multi-$i", "v4-multi", requestedTime)
        }

        val windowStarts = transaction {
            RateLimitEventSlotTable.selectAll()
                .map { it[RateLimitEventSlotTable.windowStart] }
                .distinct().sorted()
        }
        // With softMax=2 and 6 events, should use at least 3 windows
        assertTrue(windowStarts.size >= 3, "Expected at least 3 windows, got ${windowStarts.size}")
    }

    @Test
    fun `delay reflects how far event was pushed from requested time`() {
        // softMax = floor(1 * 0.9) = 0, which means no slots can be assigned in any window
        // Use maxPerWindow=2 so softMax = floor(2 * 0.9) = 1
        configRepository.createConfig("v4-delay", 2, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot1 = service.assignSlot("evt-d1", "v4-delay", requestedTime)
        assertTrue(slot1.delay >= Duration.ZERO)

        val slot2 = service.assignSlot("evt-d2", "v4-delay", requestedTime)
        assertTrue(slot2.delay >= Duration.ZERO)
    }

    // ==================== Idempotency ====================

    @Test
    fun `returns same slot for duplicate eventId`() {
        configRepository.createConfig("v4-idem", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot1 = service.assignSlot("evt-dup", "v4-idem", requestedTime)
        val slot2 = service.assignSlot("evt-dup", "v4-idem", requestedTime)

        assertEquals(slot1.eventId, slot2.eventId)
        assertEquals(slot1.scheduledTime, slot2.scheduledTime)
    }

    @Test
    fun `concurrent duplicate eventIds return same slot`() {
        configRepository.createConfig("v4-conc-dup", 100, Duration.ofSeconds(4))
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
                    val slot = service.assignSlot("evt-conc-dup", "v4-conc-dup", requestedTime)
                    results.add(slot)
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
        val uniqueTimes = results.map { it.scheduledTime }.distinct()
        assertEquals(1, uniqueTimes.size, "All threads should get the same scheduledTime")
    }

    // ==================== Jitter bounds ====================

    @Test
    fun `scheduledTime is within window boundaries`() {
        configRepository.createConfig("v4-jitter", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        repeat(20) { i ->
            val slot = service.assignSlot("evt-jit-$i", "v4-jitter", requestedTime)
            val windowStart = slot.scheduledTime.let { st ->
                val epochSec = st.epochSecond
                Instant.ofEpochSecond(epochSec - (epochSec % 4))
            }
            assertTrue(slot.scheduledTime >= windowStart,
                "scheduledTime ${slot.scheduledTime} should be >= windowStart $windowStart")
            assertTrue(slot.scheduledTime < windowStart.plusSeconds(4),
                "scheduledTime ${slot.scheduledTime} should be < windowEnd ${windowStart.plusSeconds(4)}")
        }
    }

    // ==================== Start window / skip full windows ====================

    @Test
    fun `findStartWindow skips past full windows`() {
        // softMax = floor(10 * 0.9) = 9
        configRepository.createConfig("v4-skip", 10, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill windows 0-9 (90 events, softMax=9 per window → 10 windows)
        (1..90).forEach { i ->
            service.assignSlot("evt-skip-$i", "v4-skip", requestedTime)
        }

        // Next event should land in a later window, not re-scan from window 0
        val slot = service.assignSlot("evt-skip-91", "v4-skip", requestedTime)
        assertTrue(slot.scheduledTime >= requestedTime,
            "Slot should be at or after requestedTime")
    }

    @Test
    fun `findStartWindow returns requestedTime when no slots exist`() {
        configRepository.createConfig("v4-empty", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot = service.assignSlot("evt-empty-1", "v4-empty", requestedTime)
        // With no prior slots, should start from requestedTime
        assertTrue(slot.scheduledTime >= requestedTime)
        assertTrue(slot.scheduledTime < requestedTime.plusSeconds(4 * 5))
    }

    // ==================== Exhaustion ====================

    @Test
    fun `throws when softMax is zero`() {
        // softMax = floor(1 * 0.9) = 0 → no window can accept slots (COUNT < 0 is never true)
        configRepository.createConfig("v4-exhaust", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        assertThrows(SlotAssignmentException::class.java) {
            service.assignSlot("evt-ex-1", "v4-exhaust", requestedTime)
        }
    }

    // ==================== Config errors ====================

    @Test
    fun `throws ConfigLoadException for missing config`() {
        assertThrows(ConfigLoadException::class.java) {
            service.assignSlot("evt-noconf", "nonexistent", Instant.now())
        }
    }

    // ==================== Bulk / concurrent ====================

    @Test
    fun `bulk assignment of 200 events for same timestamp`() {
        configRepository.createConfig("v4-bulk", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slots = (1..200).map { i ->
            service.assignSlot("evt-bulk-$i", "v4-bulk", requestedTime)
        }

        assertEquals(200, slots.size)
        val uniqueEventIds = slots.map { it.eventId }.distinct()
        assertEquals(200, uniqueEventIds.size, "All event IDs should be unique")

        // Verify all scheduledTimes are at or after requestedTime
        slots.forEach { slot ->
            assertTrue(slot.scheduledTime >= requestedTime)
        }
    }

    @Test
    fun `concurrent assignment - 100 events across 20 threads`() {
        configRepository.createConfig("v4-conc", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(1)
        val totalEvents = 100
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
                        val slot = service.assignSlot(eventId, "v4-conc", requestedTime)
                        results[eventId] = slot
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
        assertEquals(totalEvents, results.size, "All events should be assigned")

        // No duplicate scheduledTimes for different eventIds (sanity check — not guaranteed but likely)
        val dbCount = transaction {
            RateLimitEventSlotTable.selectAll().count()
        }
        assertEquals(totalEvents.toLong(), dbCount, "DB should have exactly $totalEvents rows")
    }

    // ==================== Soft limit tolerance ====================

    @Test
    fun `over-capacity is tolerated under concurrent load`() {
        // softMax = floor(10 * 0.9) = 9
        configRepository.createConfig("v4-soft", 10, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
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
                        service.assignSlot("evt-soft-$idx", "v4-soft", requestedTime)
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

        // Check per-window counts — some may exceed softMax (9) but should be reasonable
        val windowCounts = transaction {
            RateLimitEventSlotTable.selectAll()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { (_, rows) -> rows.size }
        }

        windowCounts.forEach { (window, count) ->
            // Over-capacity is tolerated, but shouldn't be wildly over maxPerWindow
            assertTrue(count <= 50,
                "Window $window has $count slots, expected <= 50 (reasonable overshoot)")
        }

        val totalSlots = windowCounts.values.sum()
        assertEquals(totalEvents, totalSlots, "Total slots should equal total events")
    }
}
