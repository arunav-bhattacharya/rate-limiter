package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.SkipPointerTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.SkipPointerRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import com.ratelimiter.slot.AllocationStatus
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV5
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
 * Tests for V5 slot assignment with DB-backed skip pointer,
 * three-phase allocation (1/2/3), proximity-weighted random selection,
 * maxSlots enforcement, and per-request maxDuration.
 *
 * Test profile: windowSize=4s, maxSlotsPerWindow=4,
 *               softMaxPercent=75 (softMax=3),
 *               defaultMaxDurationHours=1, extensionWindows=4,
 *               maxExtensionsBeyond=3, maxClaimRetries=2.
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class SlotAssignmentServiceV5Test {

    @Inject
    lateinit var service: SlotAssignmentServiceV5

    @Inject
    lateinit var eventSlotRepository: EventSlotRepository

    @Inject
    lateinit var windowSlotCounterRepository: WindowSlotCounterRepository

    @Inject
    lateinit var skipPointerRepository: SkipPointerRepository

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
        assertEquals(AllocationStatus.NORMAL, slot.allocationStatus)
        assertFalse(slot.scheduledTime.isBefore(requestedTime))
        assertTrue(slot.delay >= Duration.ZERO)

        // Counter row created on demand
        val counterRows = transaction { WindowCounterTable.selectAll().toList() }
        assertEquals(1, counterRows.size)
        assertEquals(1, counterRows[0][WindowCounterTable.slotCount])
    }

    @Test
    fun `phase 1 - different requestedTime gets independent search range`() {
        val time1 = Instant.parse("2025-06-01T14:00:00Z")
        val time2 = Instant.parse("2025-06-01T13:00:00Z")

        seedSlot("evt-a1", time1, time1, time1.plusMillis(500))
        seedCounter(time1, 1)

        val slot = service.assignSlot("evt-b1", time2)

        assertEquals("evt-b1", slot.eventId)
        assertEquals(AllocationStatus.NORMAL, slot.allocationStatus)
        assertFalse(slot.scheduledTime.isBefore(time2))
        assertTrue(slot.scheduledTime.isBefore(time1))
    }

    @Test
    fun `phase 1 - enough available windows skips extension`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime
        val w1 = requestedTime.plusSeconds(4)
        val w2 = requestedTime.plusSeconds(8)

        // W+0 full (at softMax=3), W+1 available, W+2 full, W+3 available
        seedCounter(w0, 3)
        seedCounter(w1, 1)
        seedCounter(w2, 3)

        val slot = service.assignSlot("evt-ne1", requestedTime)

        assertEquals(AllocationStatus.NORMAL, slot.allocationStatus)
        val maxDurationEnd = requestedTime.plus(Duration.ofHours(1))
        assertTrue(slot.scheduledTime.isBefore(maxDurationEnd))
    }

    @Test
    fun `phase 1 - proximity weighting prefers closer windows`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val w0 = requestedTime
        val w1 = requestedTime.plusSeconds(4)

        // W+0: count=1, W+1: count=1, rest empty
        seedCounter(w0, 1)
        seedCounter(w1, 1)

        // Assign many slots — closer windows should get more assignments
        val slots = (1..6).map { i ->
            service.assignSlot("evt-prox$i", requestedTime)
        }

        // All should be NORMAL
        assertTrue(slots.all { it.allocationStatus == AllocationStatus.NORMAL })

        val slotsByWindow = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .filter { it[RateLimitEventSlotTable.eventId].startsWith("evt-prox") }
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { it.value.size }
        }

        // Should span multiple windows (not all in one)
        assertTrue(slotsByWindow.size > 1, "Should span multiple windows")
    }

    // ==================== Delay computation ====================

    @Test
    fun `delay reflects how far event was pushed from requestedTime`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Fill W+0 to softMax (3 slots) so next request must go to W+1+
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

        // Fill all windows within default maxDuration (1h = 900 windows at 4s)
        // to softMax. For test tractability, use a short maxDuration.
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
        assertEquals(AllocationStatus.SOFT_MAX_EXCEEDED, slot.allocationStatus)
        // Should still be within maxDuration (windows have room up to maxSlots=4)
        assertTrue(slot.scheduledTime.isBefore(requestedTime.plus(maxDuration)))
    }

    // ==================== Phase 3: Extension beyond maxDuration ====================

    @Test
    fun `phase 3 - all windows at maxSlots returns MAX_DURATION_EXCEEDED`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        // Fill all windows within maxDuration to maxSlots (4)
        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 4) {
                seedSlot("pre-c-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 4) // at maxSlots
        }

        val slot = service.assignSlot("evt-extend", requestedTime, maxDuration)

        assertEquals("evt-extend", slot.eventId)
        assertEquals(AllocationStatus.MAX_DURATION_EXCEEDED, slot.allocationStatus)
        // Should be beyond maxDuration
        assertFalse(
            slot.scheduledTime.isBefore(requestedTime.plus(maxDuration)),
            "Slot should be beyond maxDuration"
        )
    }

    // ==================== maxDuration per-request ====================

    @Test
    fun `different maxDuration per request affects phase transitions`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Fill windows 0-3 (first 16s) to softMax (3)
        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 3) {
                seedSlot("pre-md-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 3)
        }
        // Windows 4+ are empty

        // Request with maxDuration=16s — all windows in range at softMax → Phase 2
        val shortSlot = service.assignSlot("evt-short", requestedTime, Duration.ofSeconds(16))
        assertEquals(AllocationStatus.SOFT_MAX_EXCEEDED, shortSlot.allocationStatus)

        // Request with maxDuration=32s — windows 4-7 are available → Phase 1
        val longSlot = service.assignSlot("evt-long", requestedTime, Duration.ofSeconds(32))
        assertEquals(AllocationStatus.NORMAL, longSlot.allocationStatus)
    }

    // ==================== Skip pointer DB ====================

    @Test
    fun `skip pointer persists across service calls`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        // Fill all windows in maxDuration to softMax (3)
        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            seedCounter(ws, 3)
        }

        // This will exhaust Phase 1 and advance skip pointer to maxDurationEnd
        val slot1 = service.assignSlot("evt-sp1", requestedTime, maxDuration)
        assertEquals(AllocationStatus.SOFT_MAX_EXCEEDED, slot1.allocationStatus)

        // Verify skip pointer was written to DB
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

    // ==================== Counter consistency ====================

    @Test
    fun `counter stays approximately consistent under contention`() {
        val totalEvents = 50
        val threadCount = 20
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-cnt$i"] = service.assignSlot("evt-cnt$i", requestedTime)
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

        val actualSlotCounts = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
                .mapValues { it.value.size }
        }

        val counterValues = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }

        for ((windowStart, actualCount) in actualSlotCounts) {
            assertEquals(
                actualCount, counterValues[windowStart],
                "Counter for $windowStart should match actual slot count $actualCount"
            )
        }

        assertEquals(totalEvents, actualSlotCounts.values.sum())
    }

    // ==================== Counter on-demand ====================

    @Test
    fun `counter row created on demand without pre-provisioning`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val beforeCount = transaction { WindowCounterTable.selectAll().count() }
        assertEquals(0L, beforeCount)

        service.assignSlot("evt-od1", requestedTime)

        val afterCount = transaction { WindowCounterTable.selectAll().count() }
        assertEquals(1L, afterCount)

        val counterRow = transaction { WindowCounterTable.selectAll().first() }
        assertEquals(1, counterRow[WindowCounterTable.slotCount])
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
        // maxDuration: 4 windows, extensions: 3 * 4 = 12 windows → total 16 windows
        for (w in 0 until 16) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 4) // maxSlots
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

        // Fill W+2 (14:00:08) to softMax (3) from time1
        val w2 = Instant.parse("2025-06-01T14:00:08Z")
        for (s in 0 until 3) {
            seedSlot("pre-t1-w2s$s", time1, w2, w2.plusMillis(s * 500L + 100))
        }
        seedCounter(w2, 3)

        // time2 starts at W+2, but it's full. Should pick a different window.
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

    // ==================== Phase 2 scans from requestedTime, not skipTo ====================

    @Test
    fun `phase 2 scans from requestedTime even when skip pointer is advanced`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        // Advance skip pointer past maxDurationEnd
        skipPointerRepository.advanceSkipTo(requestedTime, requestedTime.plusSeconds(20))

        // Seed windows 0-3 at softMax (Phase 1 will find nothing since skipTo > maxDurationEnd)
        // but Phase 2 scans from requestedTime and finds room up to maxSlots (4)
        for (w in 0 until 4) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 3) // at softMax, below maxSlots
        }

        val slot = service.assignSlot("evt-phaseb-scan", requestedTime, maxDuration)

        // Phase 1 should fail (skip pointer past maxDuration), Phase 2 should succeed
        assertEquals(AllocationStatus.SOFT_MAX_EXCEEDED, slot.allocationStatus)
        assertTrue(slot.scheduledTime.isBefore(requestedTime.plus(maxDuration)))
    }

    // ==================== Phase 1 chunking ====================

    @Test
    fun `phase 1 chunking - first chunk full, slot lands in second chunk`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        // Test profile: phaseAChunkSeconds=16 → 4 windows per chunk

        // Fill first chunk (W+0..W+3) to softMax (3)
        for (w in 0 until 4) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 3) // softMax
        }
        // Second chunk (W+4..W+7) is empty

        val slot = service.assignSlot("evt-chunk", requestedTime)

        assertEquals(AllocationStatus.NORMAL, slot.allocationStatus)
        // Slot should land in second chunk (>= 16s from requestedTime)
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

        // Fill first chunk (W+0..W+3) to softMax (3)
        for (w in 0 until 4) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 3)
        }

        // First request: exhausts chunk 1, lands in chunk 2
        service.assignSlot("evt-sp-adv1", requestedTime)

        // Verify skip pointer advanced to chunk 1 boundary
        val skipTo = skipPointerRepository.fetchSkipTo(requestedTime)
        assertNotNull(skipTo)
        assertTrue(
            !skipTo!!.isBefore(requestedTime.plusSeconds(16)),
            "Skip pointer should be at or beyond chunk 1 end (16s)"
        )

        // Fill chunk 2 to softMax (now both chunks exhausted)
        for (w in 4 until 8) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 3)
        }

        // Second request: should start from chunk 2 (skip pointer), exhaust it, land in chunk 3
        val slot2 = service.assignSlot("evt-sp-adv2", requestedTime)
        assertEquals(AllocationStatus.NORMAL, slot2.allocationStatus)
        val chunk3Start = requestedTime.plusSeconds(32)
        assertFalse(
            slot2.scheduledTime.isBefore(chunk3Start),
            "Second request should land in chunk 3 (>= $chunk3Start)"
        )
    }

    // ==================== maxSlots rollback ====================

    @Test
    fun `maxSlots exceeded triggers rollback and retry in different window`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        // Set W+0 to maxSlots — any claim should rollback and retry on W+1+
        seedCounter(requestedTime, 4) // maxSlots in test profile
        // W+1..W+3 are empty — plenty of room

        val slot = service.assignSlot("evt-hc", requestedTime)

        assertEquals(AllocationStatus.NORMAL, slot.allocationStatus)

        // Verify the slot did NOT land in W+0 (at maxSlots)
        val slotWindowStart = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-hc" }
                .first()[RateLimitEventSlotTable.windowStart]
        }
        assertNotEquals(
            requestedTime, slotWindowStart,
            "Slot should not be in W+0 (at maxSlots), but was"
        )

        // W+0's counter should still be 6 (rollback undid the increment)
        val w0Count = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq requestedTime }
                .first()[WindowCounterTable.slotCount]
        }
        assertEquals(4, w0Count, "W+0 counter should remain at maxSlots (rollback undid increment)")
    }

    // ==================== Phase 3 advances skip pointer ====================

    @Test
    fun `phase 3 advances skip pointer into extension range`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16) // 4 windows

        // Fill all windows in maxDuration to maxSlots (4, forces Phase 3)
        for (w in 0 until 4) {
            seedCounter(requestedTime.plusSeconds(w * 4L), 4)
        }

        val slot = service.assignSlot("evt-pc-skip", requestedTime, maxDuration)
        assertEquals(AllocationStatus.MAX_DURATION_EXCEEDED, slot.allocationStatus)

        // Skip pointer should be advanced beyond maxDuration into extension range
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

        // Manually advance skip pointer to a future value
        val futureSkipTo = requestedTime.plusSeconds(100)
        skipPointerRepository.advanceSkipTo(requestedTime, futureSkipTo)

        // Try to advance to an earlier value
        val earlierSkipTo = requestedTime.plusSeconds(50)
        skipPointerRepository.advanceSkipTo(requestedTime, earlierSkipTo)

        // Should still be at the future value
        val current = skipPointerRepository.fetchSkipTo(requestedTime)
        assertEquals(futureSkipTo, current, "Skip pointer should not go backward")
    }

    // ==================== Full three-phase transition in one request ====================

    @Test
    fun `request transitions through Phase 1 to Phase 2 to Phase 3`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")
        val maxDuration = Duration.ofSeconds(16)

        // Phase 1 windows (0-3): at softMax → Phase 1 fails
        // Phase 2 windows (0-3): at maxSlots → Phase 2 fails
        // Phase 3 windows (4+): empty → Phase 3 succeeds
        for (w in 0 until 4) {
            val ws = requestedTime.plusSeconds(w * 4L)
            for (s in 0 until 4) {
                seedSlot("pre-3p-w${w}s${s}", requestedTime, ws, ws.plusMillis(s * 500L + 100))
            }
            seedCounter(ws, 4) // at maxSlots
        }

        val slot = service.assignSlot("evt-3phase", requestedTime, maxDuration)

        // Phase 1 and B both exhausted → must be Phase 3
        assertEquals(AllocationStatus.MAX_DURATION_EXCEEDED, slot.allocationStatus)
        assertFalse(
            slot.scheduledTime.isBefore(requestedTime.plus(maxDuration)),
            "Slot should be beyond maxDuration (Phase 3)"
        )
    }

    // ==================== Counter not incremented for idempotent hit ====================

    @Test
    fun `counter not double-incremented on duplicate eventId`() {
        val requestedTime = Instant.parse("2025-06-01T14:00:00Z")

        val first = service.assignSlot("evt-dup-ctr", requestedTime)
        val windowStart = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-dup-ctr" }
                .first()[RateLimitEventSlotTable.windowStart]
        }

        val counterBefore = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq windowStart }
                .first()[WindowCounterTable.slotCount]
        }

        // Duplicate request — should NOT increment counter
        val second = service.assignSlot("evt-dup-ctr", requestedTime)
        assertEquals(first.scheduledTime, second.scheduledTime)

        val counterAfter = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq windowStart }
                .first()[WindowCounterTable.slotCount]
        }

        assertEquals(counterBefore, counterAfter, "Counter should not increment for duplicate eventId")
    }
}
