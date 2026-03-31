package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.SkipPointerTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV7
import com.ratelimiter.slot.WindowCounterRefreshJob
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tests for V7 slot assignment — single-phase, occupancy-weighted, STATUS-based.
 *
 * Test profile: windowSize=4s, maxSlotsPerWindow=4,
 *               candidateWindowCount=10.
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceV7Test {

    @Inject
    lateinit var service: SlotAssignmentServiceV7

    @Inject
    lateinit var eventSlotRepository: EventSlotRepository

    @Inject
    lateinit var windowSlotCounterRepository: WindowSlotCounterRepository

    @Inject
    lateinit var counterRefreshJob: WindowCounterRefreshJob

    private val windowSize = Duration.ofSeconds(4)
    private val maxDuration = Duration.ofHours(1)

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
            SkipPointerTable.deleteAll()
        }
    }

    private fun seedWindow(windowStart: Instant, count: Int, status: String = "AVAILABLE") {
        transaction {
            WindowCounterTable.insert {
                it[WindowCounterTable.windowStart] = windowStart
                it[slotCount] = count
                it[windowStatus] = status
                it[createdAt] = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            }
        }
    }

    private fun seedSlot(
        eventId: String,
        requestedTime: Instant,
        windowStart: Instant,
        scheduledTime: Instant
    ) {
        transaction {
            RateLimitEventSlotTable.insert {
                it[slotId] = UUID.randomUUID().toString()
                it[RateLimitEventSlotTable.eventId] = eventId
                it[RateLimitEventSlotTable.requestedTime] = requestedTime
                it[RateLimitEventSlotTable.windowStart] = windowStart
                it[RateLimitEventSlotTable.scheduledTime] = scheduledTime
                it[configId] = "STATIC"
                it[createdAt] = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            }
        }
    }

    // ==================== Basic allocation ====================

    @Test
    fun `assigns slot in available window`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime
        val w1 = requestedTime.plusSeconds(4)
        seedWindow(w0, 0)
        seedWindow(w1, 0)

        val slot = service.assignSlot("evt-1", requestedTime, maxDuration)

        assertEquals("evt-1", slot.eventId)
        assertFalse(slot.scheduledTime.isBefore(requestedTime))
        assertTrue(slot.delay >= Duration.ZERO)
    }

    @Test
    fun `assigns slot in correct range with maxDuration`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val shortDuration = Duration.ofSeconds(16) // 4 windows
        for (w in 0 until 4) {
            seedWindow(requestedTime.plusSeconds(w * 4L), 0)
        }
        // Seed windows beyond maxDuration too
        seedWindow(requestedTime.plusSeconds(16), 0)
        seedWindow(requestedTime.plusSeconds(20), 0)

        val slot = service.assignSlot("evt-range", requestedTime, shortDuration)

        assertTrue(slot.scheduledTime.isBefore(requestedTime.plus(shortDuration)),
            "Slot should be within maxDuration range")
    }

    // ==================== FULL windows skipped ====================

    @Test
    fun `skips FULL windows and assigns to AVAILABLE`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime
        val w1 = requestedTime.plusSeconds(4)
        val w2 = requestedTime.plusSeconds(8)

        seedWindow(w0, 4, "FULL")
        seedWindow(w1, 4, "FULL")
        seedWindow(w2, 1, "AVAILABLE")

        val slot = service.assignSlot("evt-skip-full", requestedTime, maxDuration)

        val slotWindowStart = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-skip-full" }
                .first()[RateLimitEventSlotTable.windowStart]
        }
        assertEquals(w2, slotWindowStart, "Should land in the AVAILABLE window (w2)")
    }

    // ==================== All FULL throws exception ====================

    @Test
    fun `throws SlotAssignmentException when all windows are FULL`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val shortDuration = Duration.ofSeconds(16)

        for (w in 0 until 4) {
            seedWindow(requestedTime.plusSeconds(w * 4L), 4, "FULL")
        }

        assertThrows(SlotAssignmentException::class.java) {
            service.assignSlot("evt-exhaust", requestedTime, shortDuration)
        }
    }

    @Test
    fun `throws SlotAssignmentException when no windows exist in range`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val shortDuration = Duration.ofSeconds(16)

        // No windows seeded at all
        assertThrows(SlotAssignmentException::class.java) {
            service.assignSlot("evt-no-windows", requestedTime, shortDuration)
        }
    }

    // ==================== Occupancy-weighted distribution ====================

    @Test
    fun `emptier windows get proportionally more assignments`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime                    // occupancy 3 → weight 1
        val w1 = requestedTime.plusSeconds(4)      // occupancy 0 → weight 4

        seedWindow(w0, 3)
        seedWindow(w1, 0)

        // Assign enough slots to see a distribution
        val eventIds = (1..8).map { "evt-occ$it" }
        eventIds.forEach { service.assignSlot(it, requestedTime, maxDuration) }

        val slotsByWindow = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .filter { it[RateLimitEventSlotTable.eventId].startsWith("evt-occ") }
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { it.value.size }
        }

        // w1 (empty) should get more assignments than w0 (nearly full)
        val w1Count = slotsByWindow[w1] ?: 0
        val w0Count = slotsByWindow[w0] ?: 0
        assertTrue(w1Count > w0Count,
            "Emptier window w1 ($w1Count slots) should get more than nearly-full w0 ($w0Count slots)")
    }

    // ==================== Idempotency ====================

    @Test
    fun `idempotent re-request returns existing slot`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        seedWindow(requestedTime, 0)

        val first = service.assignSlot("evt-idem", requestedTime, maxDuration)
        val second = service.assignSlot("evt-idem", requestedTime, maxDuration)

        assertEquals(first.eventId, second.eventId)
        assertEquals(first.scheduledTime, second.scheduledTime)
        assertEquals(first.delay, second.delay)

        val count = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-idem" }
                .count()
        }
        assertEquals(1L, count)
    }

    @Test
    fun `concurrent duplicate eventIds all return same slot`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        for (w in 0 until 5) {
            seedWindow(requestedTime.plusSeconds(w * 4L), 0)
        }
        val threadCount = 10

        val results = ConcurrentLinkedQueue<AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(threadCount) {
            executor.submit {
                try {
                    results.add(service.assignSlot("evt-race", requestedTime, maxDuration))
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS))
        executor.shutdown()
        assertTrue(errors.isEmpty(), "Expected no errors: ${errors.map { it.message }}")
        assertEquals(threadCount, results.size)
        assertEquals(1, results.map { it.scheduledTime }.distinct().size)

        val count = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-race" }
                .count()
        }
        assertEquals(1L, count)
    }

    // ==================== Jitter bounds ====================

    @Test
    fun `jitter stays within window bounds`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        for (w in 0 until 4) {
            seedWindow(requestedTime.plusSeconds(w * 4L), 0)
        }

        val slots = (1..4).map { i ->
            service.assignSlot("evt-jit$i", requestedTime, maxDuration)
        }

        for (slot in slots) {
            val slotWindowStart = transaction {
                RateLimitEventSlotTable.selectAll()
                    .where { RateLimitEventSlotTable.eventId eq slot.eventId }
                    .first()[RateLimitEventSlotTable.windowStart]
            }
            val windowEnd = slotWindowStart.plusSeconds(4)

            assertFalse(slot.scheduledTime.isBefore(slotWindowStart),
                "scheduledTime should be >= windowStart")
            assertTrue(slot.scheduledTime.isBefore(windowEnd),
                "scheduledTime should be < windowEnd")
        }
    }

    // ==================== Concurrent bulk ====================

    @Test
    fun `bulk concurrent requests distribute across windows`() {
        val totalEvents = 20
        val threadCount = 10
        val requestedTime = Instant.parse("2025-06-01T13:00:00Z")

        // Provision enough windows
        for (w in 0 until 20) {
            seedWindow(requestedTime.plusSeconds(w * 4L), 0)
        }

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-c$i"] = service.assignSlot("evt-c$i", requestedTime, maxDuration)
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS))
        executor.shutdown()
        assertTrue(errors.isEmpty(), "Expected no errors: ${errors.map { it.message }}")
        assertEquals(totalEvents, results.size)

        val windowStarts = transaction {
            RateLimitEventSlotTable.selectAll()
                .map { it[RateLimitEventSlotTable.windowStart] }
                .distinct()
        }
        assertTrue(windowStarts.size > 1, "Events should span multiple windows")
    }

    // ==================== No skip pointer writes ====================

    @Test
    fun `V7 does not write to skip pointer table`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        seedWindow(requestedTime, 0)

        service.assignSlot("evt-noskip", requestedTime, maxDuration)

        val skipPointerCount = transaction {
            SkipPointerTable.selectAll().count()
        }
        assertEquals(0L, skipPointerCount, "V7 should not write to RL_SKIP_PTR")
    }

    // ==================== Scheduler integration ====================

    @Test
    fun `scheduler marks window as FULL when max slots reached`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime

        seedWindow(w0, 0)

        // Insert 4 slots manually (maxSlotsPerWindow=4 in test config)
        val insertTime = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        for (s in 0 until 4) {
            seedSlot("pre-sched-s$s", requestedTime, w0, w0.plusMillis(s * 500L + 100))
        }

        // Run the counter refresh job
        counterRefreshJob.run(insertTime.minusSeconds(1), Instant.now())

        // Verify counter was updated
        val (slotCount, status) = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq w0 }
                .first()
                .let { it[WindowCounterTable.slotCount] to it[WindowCounterTable.windowStatus] }
        }
        assertEquals(4, slotCount, "Counter should reflect 4 slots")
        assertEquals("FULL", status, "Status should be FULL when slots >= maxSlotsPerWindow")
    }

    @Test
    fun `scheduler keeps AVAILABLE status when below max slots`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime

        seedWindow(w0, 0)

        // Insert 2 slots (below maxSlotsPerWindow=4)
        val insertTime = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        for (s in 0 until 2) {
            seedSlot("pre-avail-s$s", requestedTime, w0, w0.plusMillis(s * 500L + 100))
        }

        counterRefreshJob.run(insertTime.minusSeconds(1), Instant.now())

        val (slotCount, status) = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq w0 }
                .first()
                .let { it[WindowCounterTable.slotCount] to it[WindowCounterTable.windowStatus] }
        }
        assertEquals(2, slotCount)
        assertEquals("AVAILABLE", status, "Status should remain AVAILABLE when below max")
    }

    // ==================== Delay computation ====================

    @Test
    fun `delay reflects how far event was pushed from requestedTime`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime
        val w1 = requestedTime.plusSeconds(4)

        // W+0 full, W+1 available
        seedWindow(w0, 4, "FULL")
        seedWindow(w1, 0, "AVAILABLE")

        val slot = service.assignSlot("evt-delay", requestedTime, maxDuration)

        assertTrue(slot.delay >= Duration.ZERO, "Delay must be non-negative")
        assertEquals(
            Duration.between(requestedTime, slot.scheduledTime).let { if (it.isNegative) Duration.ZERO else it },
            slot.delay,
            "Delay must equal Duration.between(requestedTime, scheduledTime)"
        )
        assertTrue(
            slot.delay >= windowSize,
            "Delay should be at least one window (4s) since W+0 is FULL"
        )
    }

    // ==================== Over-allocation bounded ====================

    @Test
    fun `over-allocation bounded under high concurrency`() {
        val totalEvents = 50
        val threadCount = 20
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val shortDuration = Duration.ofSeconds(16) // 4 windows

        for (w in 0 until 4) {
            seedWindow(requestedTime.plusSeconds(w * 4L), 0)
        }

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-oa$i"] = service.assignSlot("evt-oa$i", requestedTime, shortDuration)
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS))
        executor.shutdown()

        val nonExhaustionErrors = errors.filter { it !is SlotAssignmentException }
        assertTrue(nonExhaustionErrors.isEmpty(),
            "No non-exhaustion errors: ${nonExhaustionErrors.map { "${it.javaClass.simpleName}: ${it.message}" }}")

        // Check: no window has vastly more slots than maxSlotsPerWindow
        val actualCounts = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .filter { it[RateLimitEventSlotTable.eventId].startsWith("evt-oa") }
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { it.value.size }
        }

        val maxSlots = 4 // test config maxSlotsPerWindow
        val maxAllowedOvershoot = threadCount // bounded by concurrency level
        for ((windowStart, count) in actualCounts) {
            assertTrue(
                count <= maxSlots + maxAllowedOvershoot,
                "Window $windowStart has $count slots — over-allocation unbounded " +
                    "(expected <= ${maxSlots + maxAllowedOvershoot})"
            )
        }
    }

    // ==================== Shared capacity across requestedTimes ====================

    @Test
    fun `shared window capacity across requestedTimes`() {
        val time1 = Instant.parse("2025-06-01T14:00:00Z")
        val time2 = Instant.parse("2025-06-01T14:00:08Z") // W+2 of time1's range

        val w2 = Instant.parse("2025-06-01T14:00:08Z")
        val w3 = Instant.parse("2025-06-01T14:00:12Z")

        // W+2 FULL
        seedWindow(w2, 4, "FULL")
        // W+3 available
        seedWindow(w3, 0, "AVAILABLE")

        val slot = service.assignSlot("evt-shared1", time2, maxDuration)

        val slotWindowStart = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-shared1" }
                .first()[RateLimitEventSlotTable.windowStart]
        }
        assertNotEquals(w2, slotWindowStart,
            "time2's slot should avoid W+2 which is FULL")
    }

    // ==================== No deadlocks ====================

    @Test
    fun `no deadlocks under sustained concurrent load`() {
        val totalEvents = 100
        val threadCount = 30
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Provision enough windows for 100 events at 4 per window
        for (w in 0 until 50) {
            seedWindow(requestedTime.plusSeconds(w * 4L), 0)
        }

        val successCount = AtomicInteger(0)
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    service.assignSlot("evt-dl$i", requestedTime, maxDuration)
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

        val nonExhaustionErrors = errors.filter { it !is SlotAssignmentException }
        assertTrue(
            nonExhaustionErrors.isEmpty(),
            "No non-exhaustion errors expected: ${nonExhaustionErrors.map { "${it.javaClass.simpleName}: ${it.message}" }}"
        )
    }
}
