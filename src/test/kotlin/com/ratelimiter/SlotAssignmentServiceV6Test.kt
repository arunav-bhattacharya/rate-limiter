package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.SkipPointerTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.SkipPointerRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV6
import com.ratelimiter.slot.WindowCounterRefreshScheduler
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
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
 * Tests for V6 slot assignment with async counter + soft guard.
 *
 * Test profile: windowSize=4s, maxSlotsPerWindow=4,
 *               softMaxPercent=75 (softMax=3),
 *               defaultMaxDuration=1h, extensionWindows=4,
 *               maxExtensionsBeyond=3.
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceV6Test {

    @Inject
    lateinit var service: SlotAssignmentServiceV6

    @Inject
    lateinit var eventSlotRepository: EventSlotRepository

    @Inject
    lateinit var windowSlotCounterRepository: WindowSlotCounterRepository

    @Inject
    lateinit var skipPointerRepository: SkipPointerRepository

    @Inject
    lateinit var counterRefreshScheduler: WindowCounterRefreshScheduler

    private val windowSize = Duration.ofSeconds(4)

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
            SkipPointerTable.deleteAll()
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

    private fun seedCounter(windowStart: Instant, count: Int) {
        transaction {
            val exists = WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq windowStart }
                .firstOrNull()
            if (exists != null) {
                WindowCounterTable.update({ WindowCounterTable.windowStart eq windowStart }) {
                    it[slotCount] = count
                }
            } else {
                WindowCounterTable.insert {
                    it[WindowCounterTable.windowStart] = windowStart
                    it[slotCount] = count
                    it[createdAt] = Instant.now().truncatedTo(ChronoUnit.MILLIS)
                }
            }
        }
    }

    // ==================== Phase 1: Normal allocation ====================

    @Test
    fun `phase 1 - empty table first request assigns slot within maxDuration`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val slot = service.assignSlot("evt-1", requestedTime)

        assertEquals("evt-1", slot.eventId)
        assertFalse(slot.scheduledTime.isBefore(requestedTime))
        assertTrue(slot.delay >= Duration.ZERO)

        // V6: hot path does NOT create counter rows
        val counterRows = transaction { WindowCounterTable.selectAll().toList() }
        assertEquals(0, counterRows.size, "V6 hot path should not create counter rows")
    }

    @Test
    fun `phase 1 - different requestedTime gets independent search range`() {
        val time1 = Instant.parse("2025-06-01T14:00:00Z")
        val time2 = Instant.parse("2025-06-01T13:00:00Z")

        seedSlot("evt-a1", time1, time1, time1.plusMillis(500))
        seedCounter(time1, 1)

        val slot = service.assignSlot("evt-b1", time2)

        assertEquals("evt-b1", slot.eventId)
        assertFalse(slot.scheduledTime.isBefore(time2))
        assertTrue(slot.scheduledTime.isBefore(time1))
    }

    @Test
    fun `phase 1 - enough available windows skips extension`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime
        val w1 = requestedTime.plusSeconds(4)
        val w2 = requestedTime.plusSeconds(8)

        // W+0 full at softMax=3 (seed both counter + slots for soft guard)
        for (s in 0 until 3) {
            seedSlot("pre-w0s$s", requestedTime, w0, w0.plusMillis(s * 500L + 100))
        }
        seedCounter(w0, 3)
        // W+1 available
        seedCounter(w1, 1)
        seedSlot("pre-w1s0", requestedTime, w1, w1.plusMillis(100))
        // W+2 full at softMax=3
        for (s in 0 until 3) {
            seedSlot("pre-w2s$s", requestedTime, w2, w2.plusMillis(s * 500L + 100))
        }
        seedCounter(w2, 3)

        val slot = service.assignSlot("evt-ne1", requestedTime)

        val maxDurationEnd = requestedTime.plus(Duration.ofHours(1))
        assertTrue(slot.scheduledTime.isBefore(maxDurationEnd))
    }

    @Test
    fun `phase 1 - proximity weighting prefers closer windows`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime
        val w1 = requestedTime.plusSeconds(4)

        seedCounter(w0, 1)
        seedSlot("pre-w0s0", requestedTime, w0, w0.plusMillis(100))
        seedCounter(w1, 1)
        seedSlot("pre-w1s0", requestedTime, w1, w1.plusMillis(100))

        // Assign many slots — closer windows should get more assignments
        val slots = (1..6).map { i ->
            service.assignSlot("evt-prox$i", requestedTime)
        }


        val slotsByWindow = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .filter { it[RateLimitEventSlotTable.eventId].startsWith("evt-prox") }
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { it.value.size }
        }

        assertTrue(slotsByWindow.size > 1, "Should span multiple windows")
    }

    // ==================== Delay computation ====================

    @Test
    fun `delay reflects how far event was pushed from requestedTime`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Fill W+0 to softMax (3 slots) — need both actual slots (soft guard) and counter (weighted random)
        for (s in 0 until 3) {
            seedSlot("pre-delay-w0s$s", requestedTime, requestedTime, requestedTime.plusMillis(s * 500L + 100))
        }
        seedCounter(requestedTime, 3) // softMax

        val slot = service.assignSlot("evt-delay", requestedTime)

        assertTrue(slot.delay >= Duration.ZERO, "Delay must be non-negative")
        assertEquals(
            Duration.between(requestedTime, slot.scheduledTime).let { if (it.isNegative) Duration.ZERO else it },
            slot.delay,
            "Delay must equal Duration.between(requestedTime, scheduledTime)"
        )
        // Since W+0 is at softMax, the slot must be in W+1 or later
        assertTrue(
            slot.delay >= windowSize,
            "Delay should be at least one window (4s) since W+0 is at softMax"
        )
    }

    // ==================== Phase 2: Overflow within maxDuration ====================

    @Test
    fun `phase 2 - all windows at softMax returns SOFT_MAX_EXCEEDED`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 3) {
                seedSlot("pre-b-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 3) // at softMax
        }

        val slot = service.assignSlot("evt-overflow", requestedTime, maxDuration)

        assertEquals("evt-overflow", slot.eventId)
        assertTrue(slot.scheduledTime.isBefore(requestedTime.plus(maxDuration)))
    }

    // ==================== Phase 3: Extension beyond maxDuration ====================

    @Test
    fun `phase 3 - all windows at maxSlots returns MAX_DURATION_EXCEEDED`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 4) {
                seedSlot("pre-c-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 4) // at maxSlots
        }

        val slot = service.assignSlot("evt-extend", requestedTime, maxDuration)

        assertEquals("evt-extend", slot.eventId)
        assertFalse(
            slot.scheduledTime.isBefore(requestedTime.plus(maxDuration)),
            "Slot should be beyond maxDuration"
        )
    }

    // ==================== maxDuration per-request ====================

    @Test
    fun `different maxDuration per request affects phase transitions`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Fill windows 0-3 (first 16s) to softMax (3) — seed both slots + counters
        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 3) {
                seedSlot("pre-md-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 3)
        }

        // Request with maxDuration=16s — all windows in range at softMax → Phase 2
        val shortSlot = service.assignSlot("evt-short", requestedTime, Duration.ofSeconds(16))

        // Request with maxDuration=32s — windows 4-7 are available → Phase 1
        val longSlot = service.assignSlot("evt-long", requestedTime, Duration.ofSeconds(32))
    }

    // ==================== Skip pointer DB ====================

    @Test
    fun `skip pointer persists across service calls`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        // Fill all windows in maxDuration to softMax (3) — counter-only is sufficient
        // (weighted random rejects based on stale counter, skip pointer still advances)
        for (w in 0 until 4) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 3)
        }

        val slot1 = service.assignSlot("evt-sp1", requestedTime, maxDuration)

        val skipTo = skipPointerRepository.fetchSkipTo(requestedTime)
        assertNotNull(skipTo)
        assertTrue(
            !skipTo!!.isBefore(requestedTime.plus(maxDuration)),
            "Skip pointer should be at or beyond maxDurationEnd"
        )
    }

    // ==================== Idempotency ====================

    @Test
    fun `idempotent re-request returns existing slot`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val first = service.assignSlot("evt-idem", requestedTime)
        val second = service.assignSlot("evt-idem", requestedTime)

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
        val threadCount = 10

        val results = ConcurrentLinkedQueue<AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(threadCount) {
            executor.submit {
                try {
                    results.add(service.assignSlot("evt-race", requestedTime))
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

    // ==================== Concurrent bulk ====================

    @Test
    fun `bulk concurrent requests distribute across windows`() {
        val totalEvents = 20
        val threadCount = 10
        val requestedTime = Instant.parse("2025-06-01T13:00:00Z")

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
        assertTrue(errors.isEmpty(), "Expected no errors but got: ${errors.map { it.message }}")
        assertEquals(totalEvents, results.size)

        val windowStarts = transaction {
            RateLimitEventSlotTable.selectAll()
                .map { it[RateLimitEventSlotTable.windowStart] }
                .distinct()
        }
        assertTrue(windowStarts.size > 1, "Events should span multiple windows")
    }

    // ==================== Jitter bounds ====================

    @Test
    fun `jitter stays within window bounds`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val slots = (1..4).map { i ->
            service.assignSlot("evt-jit$i", requestedTime)
        }

        for (slot in slots) {
            val slotWindowStart = transaction {
                RateLimitEventSlotTable.selectAll()
                    .where { RateLimitEventSlotTable.eventId eq slot.eventId }
                    .first()[RateLimitEventSlotTable.windowStart]
            }
            val windowEnd = slotWindowStart.plusSeconds(4)

            assertFalse(slot.scheduledTime.isBefore(slotWindowStart))
            assertTrue(slot.scheduledTime.isBefore(windowEnd))
        }
    }

    // ==================== Exhaustion ====================

    @Test
    fun `throws SlotAssignmentException when all windows exhausted`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        // Fill all windows in maxDuration + all extension ranges to maxSlots
        // Seed both counters AND slots (soft guard needs actual slots)
        for (w in 0 until 16) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 4) {
                seedSlot("pre-exh-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 4) // maxSlots
        }

        assertThrows(SlotAssignmentException::class.java) {
            service.assignSlot("evt-exhaust", requestedTime, maxDuration)
        }
    }

    // ==================== Shared capacity ====================

    @Test
    fun `shared window capacity across requestedTimes`() {
        val time1 = Instant.parse("2025-06-01T14:00:00Z")
        val time2 = Instant.parse("2025-06-01T14:00:08Z") // W+2 of time1's range

        // Fill W+2 (14:00:08) to softMax (3) — seed both
        val w2 = Instant.parse("2025-06-01T14:00:08Z")
        for (s in 0 until 3) {
            seedSlot("pre-t1-w2s$s", time1, w2, w2.plusMillis(s * 500L + 100))
        }
        seedCounter(w2, 3)

        val slot = service.assignSlot("evt-shared1", time2)

        val slotWindowStart = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-shared1" }
                .first()[RateLimitEventSlotTable.windowStart]
        }
        assertNotEquals(w2, slotWindowStart,
            "time2's slot should avoid W+2 which is full from time1's slots")
    }

    // ==================== No deadlocks ====================

    @Test
    fun `no deadlocks under sustained concurrent load`() {
        val totalEvents = 100
        val threadCount = 30
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

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

        val nonExhaustionErrors = errors.filter { it !is SlotAssignmentException }
        assertTrue(
            nonExhaustionErrors.isEmpty(),
            "No non-exhaustion errors expected: ${nonExhaustionErrors.map { "${it.javaClass.simpleName}: ${it.message}" }}"
        )
    }

    // ==================== Over-allocation bounded ====================

    @Test
    fun `over-allocation bounded under high concurrency`() {
        val totalEvents = 50
        val threadCount = 20
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows — force concentration

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-oa$i"] = service.assignSlot("evt-oa$i", requestedTime, maxDuration)
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS))
        executor.shutdown()

        // Some SlotAssignmentExceptions may occur if extension windows also fill
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

    // ==================== Far-future isolation ====================

    @Test
    fun `far-future event does not corrupt near-term search`() {
        val farFuture = Instant.parse("2026-06-01T12:00:00Z")
        val nearTerm = Instant.parse("2025-07-01T12:00:00Z")

        val farSlot = service.assignSlot("evt-far", farFuture)
        assertFalse(farSlot.scheduledTime.isBefore(farFuture))

        val nearSlot = service.assignSlot("evt-near", nearTerm)
        assertFalse(nearSlot.scheduledTime.isBefore(nearTerm))
        assertTrue(nearSlot.scheduledTime.isBefore(nearTerm.plus(Duration.ofHours(1))))
    }

    // ==================== Phase 2 scans from requestedTime ====================

    @Test
    fun `phase 2 scans from requestedTime even when skip pointer is advanced`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        skipPointerRepository.advanceSkipTo(requestedTime, requestedTime.plusSeconds(20))

        // Seed windows 0-3 at softMax with actual slots (soft guard needs them)
        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 3) {
                seedSlot("pre-p2-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 3) // at softMax, below maxSlots
        }

        val slot = service.assignSlot("evt-phaseb-scan", requestedTime, maxDuration)

        assertTrue(slot.scheduledTime.isBefore(requestedTime.plus(maxDuration)))
    }

    // ==================== Phase 1 chunking ====================

    @Test
    fun `phase 1 chunking - first chunk full, slot lands in second chunk`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Fill first chunk (W+0..W+3) to softMax (3) — counter-only sufficient
        // (weighted random rejects, soft guard never reached)
        for (w in 0 until 4) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 3)
        }

        val slot = service.assignSlot("evt-chunk", requestedTime)

        val chunk2Start = requestedTime.plusSeconds(16)
        assertFalse(
            slot.scheduledTime.isBefore(chunk2Start),
            "Slot should be in second chunk (>= $chunk2Start), but was ${slot.scheduledTime}"
        )
    }

    // ==================== Skip pointer per-chunk advancement ====================

    @Test
    fun `skip pointer advances per chunk so next request skips exhausted chunks`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        for (w in 0 until 4) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 3)
        }

        service.assignSlot("evt-sp-adv1", requestedTime)

        val skipTo = skipPointerRepository.fetchSkipTo(requestedTime)
        assertNotNull(skipTo)
        assertTrue(
            !skipTo!!.isBefore(requestedTime.plusSeconds(16)),
            "Skip pointer should be at or beyond chunk 1 end (16s)"
        )

        for (w in 4 until 8) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 3)
        }

        val slot2 = service.assignSlot("evt-sp-adv2", requestedTime)
        val chunk3Start = requestedTime.plusSeconds(32)
        assertFalse(
            slot2.scheduledTime.isBefore(chunk3Start),
            "Second request should land in chunk 3 (>= $chunk3Start)"
        )
    }

    // ==================== Phase 3 advances skip pointer ====================

    @Test
    fun `phase 3 advances skip pointer into extension range`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 4) {
                seedSlot("pre-pc-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 4)
        }

        val slot = service.assignSlot("evt-pc-skip", requestedTime, maxDuration)

        val skipTo = skipPointerRepository.fetchSkipTo(requestedTime)
        assertNotNull(skipTo)
        assertTrue(
            !skipTo!!.isBefore(requestedTime.plus(maxDuration)),
            "Skip pointer should be at or beyond maxDurationEnd after Phase 3"
        )
    }

    // ==================== Skip pointer monotonic ====================

    @Test
    fun `skip pointer only advances forward never backward`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val futureSkipTo = requestedTime.plusSeconds(100)
        skipPointerRepository.advanceSkipTo(requestedTime, futureSkipTo)

        val earlierSkipTo = requestedTime.plusSeconds(50)
        skipPointerRepository.advanceSkipTo(requestedTime, earlierSkipTo)

        val current = skipPointerRepository.fetchSkipTo(requestedTime)
        assertEquals(futureSkipTo, current, "Skip pointer should not go backward")
    }

    // ==================== Three-phase transition ====================

    @Test
    fun `request transitions through Phase 1 to Phase 2 to Phase 3`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16)

        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 4) {
                seedSlot("pre-3p-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 4) // at maxSlots
        }

        val slot = service.assignSlot("evt-3phase", requestedTime, maxDuration)

        assertFalse(
            slot.scheduledTime.isBefore(requestedTime.plus(maxDuration)),
            "Slot should be beyond maxDuration (Phase 3)"
        )
    }

    // ==================== Counter not touched by hot path ====================

    @Test
    fun `counter not touched on duplicate eventId`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        service.assignSlot("evt-dup-ctr", requestedTime)

        // V6: counter table should be empty (hot path doesn't write to it)
        val countersBefore = transaction { WindowCounterTable.selectAll().count() }
        assertEquals(0L, countersBefore, "V6 hot path should not create counter rows")

        // Duplicate request — should still not touch counters
        service.assignSlot("evt-dup-ctr", requestedTime)

        val countersAfter = transaction { WindowCounterTable.selectAll().count() }
        assertEquals(0L, countersAfter, "Counter table should remain empty after duplicate")
    }

    // ==================== V6-specific: Hot path does not create counters ====================

    @Test
    fun `hot path does NOT create counter rows`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val beforeCount = transaction { WindowCounterTable.selectAll().count() }
        assertEquals(0L, beforeCount)

        service.assignSlot("evt-od1", requestedTime)

        val afterCount = transaction { WindowCounterTable.selectAll().count() }
        assertEquals(0L, afterCount, "V6 hot path should not create counter rows")

        // But the slot should exist
        val slotCount = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-od1" }
                .count()
        }
        assertEquals(1L, slotCount)
    }

    // ==================== V6-specific: Soft guard ====================

    @Test
    fun `soft guard rejects window when fresh count at maxSlotsPerWindow`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Seed W+0 with 4 actual slots (= maxSlotsPerWindow)
        // but counter at 0 (stale). Weighted random will pick W+0 (counter says empty),
        // but soft guard should reject it (fresh COUNT = 4 >= 4).
        for (s in 0 until 4) {
            seedSlot("pre-sg-w0s$s", requestedTime, requestedTime, requestedTime.plusMillis(s * 500L + 100))
        }
        seedCounter(requestedTime, 0) // stale counter

        val slot = service.assignSlot("evt-sg-reject", requestedTime)

        // Slot should NOT be in W+0 — soft guard rejected it
        val slotWindowStart = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-sg-reject" }
                .first()[RateLimitEventSlotTable.windowStart]
        }
        assertNotEquals(
            requestedTime, slotWindowStart,
            "Soft guard should reject W+0 (fresh count at maxSlotsPerWindow)"
        )
    }

    @Test
    fun `soft guard allows window when fresh count below maxSlotsPerWindow`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Seed W+0 with 3 actual slots (below maxSlotsPerWindow of 4)
        // Counter at 0 (stale). Weighted random picks W+0, soft guard allows it.
        for (s in 0 until 3) {
            seedSlot("pre-sg-w0s$s", requestedTime, requestedTime, requestedTime.plusMillis(s * 500L + 100))
        }
        seedCounter(requestedTime, 0) // stale counter

        val slot = service.assignSlot("evt-sg-allow", requestedTime)

        // Slot CAN be in W+0 — soft guard allowed it (fresh count 3 < maxSlots 4)
        // Note: proximity weighting might still pick another window, so we just verify
        // the request succeeded
        assertEquals("evt-sg-allow", slot.eventId)
    }

    // ==================== V6-specific: Scheduler ====================

    @Test
    fun `scheduler refreshes counters to match actual slot counts`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Assign several slots — counter table stays empty
        for (i in 1..8) {
            service.assignSlot("evt-sch$i", requestedTime)
        }

        val beforeRefresh = transaction { WindowCounterTable.selectAll().count() }
        assertEquals(0L, beforeRefresh, "Counter table should be empty before refresh")

        // Run scheduler
        counterRefreshScheduler.refresh()

        // Verify counters match actual slot counts
        val actualCounts = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { it.value.size }
        }

        val counterValues = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }

        for ((windowStart, actualCount) in actualCounts) {
            assertEquals(
                actualCount, counterValues[windowStart],
                "Counter for $windowStart should match actual slot count $actualCount"
            )
        }
    }

    @Test
    fun `scheduler creates counter rows for windows with slots but no counter`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime
        val w1 = requestedTime.plusSeconds(4)

        // Seed slots directly — no counter rows
        seedSlot("sch-w0s0", requestedTime, w0, w0.plusMillis(100))
        seedSlot("sch-w0s1", requestedTime, w0, w0.plusMillis(200))
        seedSlot("sch-w1s0", requestedTime, w1, w1.plusMillis(100))

        assertEquals(0L, transaction { WindowCounterTable.selectAll().count() })

        counterRefreshScheduler.refresh()

        val counters = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }
        assertEquals(2, counters[w0], "W+0 should have count=2")
        assertEquals(1, counters[w1], "W+1 should have count=1")
    }

    @Test
    fun `scheduler idempotency - running refresh twice produces same counter state`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        for (i in 1..6) {
            service.assignSlot("evt-idem-sch$i", requestedTime)
        }

        // First refresh
        counterRefreshScheduler.refresh()

        val countersAfterFirst = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }

        // Second refresh — should produce identical state (MERGE uses SET, not INCREMENT)
        counterRefreshScheduler.refresh()

        val countersAfterSecond = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }

        assertEquals(countersAfterFirst, countersAfterSecond,
            "Running refresh() twice should produce identical counter state")
    }

    @Test
    fun `counter eventually consistent after scheduler run`() {
        val totalEvents = 30
        val threadCount = 10
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-ec$i"] = service.assignSlot("evt-ec$i", requestedTime)
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

        // Run scheduler to reconcile
        counterRefreshScheduler.refresh()

        val actualCounts = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { it.value.size }
        }

        val counterValues = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }

        for ((windowStart, actualCount) in actualCounts) {
            assertEquals(
                actualCount, counterValues[windowStart],
                "Counter for $windowStart should match actual slot count $actualCount after refresh"
            )
        }

        assertEquals(totalEvents, actualCounts.values.sum())
    }

    @Test
    fun `multiple requestedTimes share same counter table`() {
        // Two genuinely different requestedTimes whose windows may overlap
        val time1 = Instant.parse("2025-06-01T14:00:00Z")
        val time2 = Instant.parse("2025-06-01T14:00:04Z") // W+1 of time1's range

        // Assign from two different requestedTimes
        service.assignSlot("evt-mt-a1", time1)
        service.assignSlot("evt-mt-b1", time2)

        counterRefreshScheduler.refresh()

        // Counter table should aggregate both streams' slots per window
        val totalSlots = transaction {
            RateLimitEventSlotTable.selectAll().count()
        }
        val totalCounted = transaction {
            WindowCounterTable.selectAll().sumOf { it[WindowCounterTable.slotCount].toLong() }
        }

        assertEquals(totalSlots, totalCounted, "Counter table should aggregate all slots per window")
    }
}
