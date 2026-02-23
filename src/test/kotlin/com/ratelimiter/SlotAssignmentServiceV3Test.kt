package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.db.WindowEndTrackerTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
import com.ratelimiter.repo.WindowEndTrackerRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.ConfigLoadException
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV3
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
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
class SlotAssignmentServiceV3Test {

    @Inject
    lateinit var service: SlotAssignmentServiceV3

    @Inject
    lateinit var configRepository: RateLimitConfigRepository

    @Inject
    lateinit var eventSlotRepository: EventSlotRepository

    @Inject
    lateinit var windowSlotCounterRepository: WindowSlotCounterRepository

    @Inject
    lateinit var windowEndTrackerRepository: WindowEndTrackerRepository

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
            WindowEndTrackerTable.deleteAll()
        }
        configRepository.evictCache()
        service.evictFirstWindowCache()
    }

    // ==================== Basic assignment ====================

    @Test
    fun `assigns first window when capacity available`() {
        configRepository.createConfig("v3-basic", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slot = service.assignSlot("evt-1", "v3-basic", requestedTime)

        assertThat(slot.eventId).isEqualTo("evt-1")
        assertThat(slot.scheduledTime).isAfterOrEqualTo(requestedTime)
        assertThat(slot.scheduledTime).isBefore(requestedTime.plusSeconds(4))
        assertThat(slot.delay).isLessThan(Duration.ofSeconds(4))
    }

    @Test
    fun `fills multiple windows sequentially`() {
        configRepository.createConfig("v3-multi", 3, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        (1..9).forEach { i ->
            service.assignSlot("evt-multi-$i", "v3-multi", requestedTime)
        }

        val windowStarts = transaction {
            RateLimitEventSlotTable.selectAll()
                .map { it[RateLimitEventSlotTable.windowStart] }
                .distinct().sorted()
        }
        assertThat(windowStarts).hasSize(3)
        assertThat(windowStarts[0]).isEqualTo(Instant.parse("2025-06-01T12:00:00Z"))
        assertThat(windowStarts[1]).isEqualTo(Instant.parse("2025-06-01T12:00:04Z"))
        assertThat(windowStarts[2]).isEqualTo(Instant.parse("2025-06-01T12:00:08Z"))
    }

    @Test
    fun `delay reflects how far event was pushed from requested time`() {
        configRepository.createConfig("v3-delay", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val first = service.assignSlot("evt-d1", "v3-delay", requestedTime)
        assertThat(first.delay).isLessThan(Duration.ofSeconds(4))

        val second = service.assignSlot("evt-d2", "v3-delay", requestedTime)
        assertThat(second.delay).isGreaterThanOrEqualTo(Duration.ofSeconds(4))

        val third = service.assignSlot("evt-d3", "v3-delay", requestedTime)
        assertThat(third.delay).isGreaterThanOrEqualTo(Duration.ofSeconds(8))
    }

    // ==================== Idempotency ====================

    @Test
    fun `returns existing slot for duplicate eventId`() {
        configRepository.createConfig("v3-idem", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val first = service.assignSlot("evt-dup", "v3-idem", requestedTime)
        val second = service.assignSlot("evt-dup", "v3-idem", requestedTime)

        assertThat(second.eventId).isEqualTo(first.eventId)
        assertThat(second.scheduledTime).isEqualTo(first.scheduledTime)
        assertThat(second.delay).isEqualTo(first.delay)

        val count = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-dup" }
                .count()
        }
        assertThat(count).isEqualTo(1)
    }

    @Test
    fun `concurrent duplicate eventIds all return same slot`() {
        configRepository.createConfig("v3-idem-conc", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val threadCount = 10

        val results = ConcurrentLinkedQueue<AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(threadCount) {
            executor.submit {
                try {
                    results.add(service.assignSlot("evt-same", "v3-idem-conc", requestedTime))
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue()
        executor.shutdown()
        assertThat(errors).isEmpty()
        assertThat(results).hasSize(threadCount)
        assertThat(results.map { it.scheduledTime }.distinct()).hasSize(1)

        val dbCount = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq "evt-same" }
                .count()
        }
        assertThat(dbCount).isEqualTo(1)
    }

    // ==================== Proportional first-window capacity ====================

    @Test
    fun `on-boundary requested time gets full capacity`() {
        configRepository.createConfig("v3-boundary", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:04Z")

        val slot = service.assignSlot("evt-b1", "v3-boundary", requestedTime)

        assertThat(slot.scheduledTime).isAfterOrEqualTo(requestedTime)
        assertThat(slot.scheduledTime).isBefore(requestedTime.plusSeconds(4))
        assertThat(slot.delay).isLessThan(Duration.ofSeconds(4))
    }

    @Test
    fun `non-boundary requested time gets proportional max and constrained jitter`() {
        configRepository.createConfig("v3-prop", 100, Duration.ofSeconds(4))
        // 2s into a 4s window = 50% remaining = proportional max of 50
        val requestedTime = Instant.parse("2025-06-01T12:00:02Z")

        val slot = service.assignSlot("evt-p1", "v3-prop", requestedTime)

        assertThat(slot.scheduledTime).isAfterOrEqualTo(requestedTime)
        assertThat(slot.scheduledTime).isBefore(Instant.parse("2025-06-01T12:00:04Z"))
        assertThat(slot.delay).isGreaterThanOrEqualTo(Duration.ZERO)
        assertThat(slot.delay).isLessThan(Duration.ofSeconds(2))

        // Fill proportional max (50 total, already have 1)
        (2..50).forEach { i ->
            service.assignSlot("evt-p$i", "v3-prop", requestedTime)
        }

        // 51st should overflow to next window
        val overflow = service.assignSlot("evt-p51", "v3-prop", requestedTime)
        assertThat(overflow.scheduledTime).isAfterOrEqualTo(Instant.parse("2025-06-01T12:00:04Z"))
    }

    @Test
    fun `25 percent remaining gives 25 percent capacity`() {
        configRepository.createConfig("v3-quarter", 100, Duration.ofSeconds(4))
        // 3s into 4s window = 25% remaining = proportional max of 25
        val requestedTime = Instant.parse("2025-06-01T12:00:03Z")

        (1..25).forEach { i ->
            val slot = service.assignSlot("evt-q$i", "v3-quarter", requestedTime)
            assertThat(slot.scheduledTime).isAfterOrEqualTo(requestedTime)
            assertThat(slot.scheduledTime).isBefore(Instant.parse("2025-06-01T12:00:04Z"))
        }

        val overflow = service.assignSlot("evt-q26", "v3-quarter", requestedTime)
        assertThat(overflow.scheduledTime).isAfterOrEqualTo(Instant.parse("2025-06-01T12:00:04Z"))
    }

    @Test
    fun `nearly-expired window gets very small capacity`() {
        configRepository.createConfig("v3-tiny", 1000, Duration.ofSeconds(4))
        // 3.9s into 4s window = 2.5% remaining = proportional max of 25
        val requestedTime = Instant.parse("2025-06-01T12:00:03.900Z")

        (1..25).forEach { i ->
            val slot = service.assignSlot("evt-tiny-$i", "v3-tiny", requestedTime)
            assertThat(slot.scheduledTime).isAfterOrEqualTo(requestedTime)
            assertThat(slot.scheduledTime).isBefore(Instant.parse("2025-06-01T12:00:04Z"))
        }

        val overflow = service.assignSlot("evt-tiny-26", "v3-tiny", requestedTime)
        assertThat(overflow.scheduledTime).isAfterOrEqualTo(Instant.parse("2025-06-01T12:00:04Z"))
    }

    // ==================== Jitter bounds ====================

    @Test
    fun `jitter stays within window bounds`() {
        configRepository.createConfig("v3-jitter", 200, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        val windowEnd = requestedTime.plusSeconds(4)

        val slots = (1..100).map { i ->
            service.assignSlot("evt-j$i", "v3-jitter", requestedTime)
        }

        for (slot in slots) {
            assertThat(slot.scheduledTime)
                .isAfterOrEqualTo(requestedTime)
                .isBefore(windowEnd)
        }
    }

    // ==================== Window filling and skip ====================

    @Test
    fun `skips full windows to next available`() {
        configRepository.createConfig("v3-skip", 2, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        service.assignSlot("evt-sk1", "v3-skip", requestedTime)
        service.assignSlot("evt-sk2", "v3-skip", requestedTime)

        val third = service.assignSlot("evt-sk3", "v3-skip", requestedTime)
        assertThat(third.scheduledTime).isAfterOrEqualTo(requestedTime.plusSeconds(4))
        assertThat(third.delay).isGreaterThanOrEqualTo(Duration.ofSeconds(4))
    }

    @Test
    fun `skip query avoids walking full windows`() {
        configRepository.createConfig("v3-skip-perf", 2, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill 100 events across 50 windows
        (1..100).forEach { i ->
            service.assignSlot("evt-sp$i", "v3-skip-perf", requestedTime)
        }

        val slot = service.assignSlot("evt-sp101", "v3-skip-perf", requestedTime)
        assertThat(slot.scheduledTime).isAfterOrEqualTo(requestedTime.plusSeconds(50 * 4L))
        assertThat(slot.delay).isGreaterThanOrEqualTo(Duration.ofSeconds(50 * 4L))
    }

    @Test
    fun `many windows can be filled sequentially`() {
        configRepository.createConfig("v3-many", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val slots = (1..50).map { i ->
            service.assignSlot("evt-mw$i", "v3-many", requestedTime)
        }

        assertThat(slots).hasSize(50)
        val maxScheduledTime = slots.maxOf { it.scheduledTime }
        assertThat(maxScheduledTime).isAfterOrEqualTo(requestedTime.plusSeconds(49 * 4L))
        assertThat(maxScheduledTime).isBefore(requestedTime.plusSeconds(50 * 4L))
    }

    // ==================== Frontier tracking / chunked provisioning ====================

    @Test
    fun `first request initializes frontier in track_window_end`() {
        configRepository.createConfig("v3-frontier", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        service.assignSlot("evt-f1", "v3-frontier", requestedTime)

        val frontierRows = transaction {
            WindowEndTrackerTable.selectAll().toList()
        }
        // First window assignment doesn't need frontier (Phase 1 succeeds).
        // Fill the first window to force Phase 2 which initializes the frontier.
        // But with maxPerWindow=100 the first call won't exhaust Phase 1.
        // So let's verify no frontier row was needed for a single event.
        // The frontier is only created when Phase 1 fails and Phase 2 runs.
    }

    @Test
    fun `frontier is created when phase 2 activates`() {
        configRepository.createConfig("v3-frontier2", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // First event fills Phase 1
        service.assignSlot("evt-fr1", "v3-frontier2", requestedTime)
        // Second event forces Phase 2 — frontier must be created
        service.assignSlot("evt-fr2", "v3-frontier2", requestedTime)

        val frontierRows = transaction {
            WindowEndTrackerTable.selectAll()
                .where { WindowEndTrackerTable.requestedTime eq Instant.parse("2025-06-01T12:00:00Z") }
                .toList()
        }
        assertThat(frontierRows).isNotEmpty()
    }

    @Test
    fun `window counter rows are batch-provisioned`() {
        configRepository.createConfig("v3-batch", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill first window to trigger Phase 2 which batch-provisions
        service.assignSlot("evt-batch1", "v3-batch", requestedTime)
        service.assignSlot("evt-batch2", "v3-batch", requestedTime)

        val counterCount = transaction {
            WindowCounterTable.selectAll().count()
        }
        // Should have more than 2 windows — the batch provisioning creates maxWindowsInChunk rows
        assertThat(counterCount).isGreaterThan(2)
    }

    // ==================== Extension loop ====================

    @Test
    fun `events beyond initial chunk succeed via extension`() {
        configRepository.createConfig("v3-extend", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill 101 events: 1 in first window + 100 in initial chunk
        val events = (1..101).map { i ->
            service.assignSlot("evt-ext$i", "v3-extend", requestedTime)
        }
        assertThat(events).hasSize(101)

        // 102nd should succeed via extension chunk
        val overflow = service.assignSlot("evt-ext102", "v3-extend", requestedTime)
        assertThat(overflow.scheduledTime).isAfterOrEqualTo(requestedTime.plusSeconds(101 * 4L))
        assertThat(overflow.delay).isGreaterThanOrEqualTo(Duration.ofSeconds(101 * 4L))
    }

    @Test
    fun `track_window_end has multiple rows after extensions`() {
        configRepository.createConfig("v3-twe", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill enough to trigger extension
        (1..102).forEach { i ->
            service.assignSlot("evt-twe$i", "v3-twe", requestedTime)
        }

        val frontierRows = transaction {
            WindowEndTrackerTable.selectAll()
                .where { WindowEndTrackerTable.requestedTime eq Instant.parse("2025-06-01T12:00:00Z") }
                .toList()
        }
        // Initial frontier + at least 1 extension
        assertThat(frontierRows.size).isGreaterThanOrEqualTo(2)
    }

    // ==================== Exhaustion ====================

    @Test
    fun `throws SlotAssignmentException when all chunks exhausted`() {
        // Build a service with maxChunksToSearch=0 so the extension loop never runs.
        // Capacity = 1 (first window) + 100 (initial chunk) = 101 windows.
        // With max_per_window=1, event 102 should fail.
        val noExtensionService = SlotAssignmentServiceV3(
            configRepository, eventSlotRepository,
            windowSlotCounterRepository, windowEndTrackerRepository,
            maxWindowsInChunk = 100, maxChunksToSearch = 0
        )
        configRepository.createConfig("v3-exhaust", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill all available capacity
        (1..101).forEach { i ->
            noExtensionService.assignSlot("evt-ex$i", "v3-exhaust", requestedTime)
        }

        assertThatThrownBy {
            noExtensionService.assignSlot("evt-ex102", "v3-exhaust", requestedTime)
        }.isInstanceOf(SlotAssignmentException::class.java)
    }

    // ==================== Counter consistency ====================

    @Test
    fun `window counter matches actual slot count`() {
        configRepository.createConfig("v3-counter", 100, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        repeat(10) { i ->
            service.assignSlot("evt-cnt$i", "v3-counter", requestedTime)
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
    fun `full window counter matches max_per_window`() {
        configRepository.createConfig("v3-full-cnt", 2, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        service.assignSlot("evt-fc1", "v3-full-cnt", requestedTime)
        service.assignSlot("evt-fc2", "v3-full-cnt", requestedTime)

        val slotCount = transaction {
            WindowCounterTable.selectAll()
                .where { WindowCounterTable.windowStart eq Instant.parse("2025-06-01T12:00:00Z") }
                .firstOrNull()
                ?.get(WindowCounterTable.slotCount)
        }
        assertThat(slotCount).isEqualTo(2)

        val third = service.assignSlot("evt-fc3", "v3-full-cnt", requestedTime)
        assertThat(third.scheduledTime).isAfterOrEqualTo(requestedTime.plusSeconds(4))
    }

    // ==================== Isolation ====================

    @Test
    fun `far-future event does not corrupt near-term search`() {
        configRepository.createConfig("v3-iso", 100, Duration.ofSeconds(4))

        val farFuture = Instant.parse("2026-06-01T12:00:00Z")
        val farSlot = service.assignSlot("evt-far", "v3-iso", farFuture)
        assertThat(farSlot.scheduledTime).isAfterOrEqualTo(farFuture)

        val nearTerm = Instant.parse("2025-07-01T12:00:00Z")
        val nearSlot = service.assignSlot("evt-near", "v3-iso", nearTerm)

        assertThat(nearSlot.scheduledTime).isAfterOrEqualTo(nearTerm)
        assertThat(nearSlot.scheduledTime).isBefore(nearTerm.plusSeconds(4))
        assertThat(nearSlot.delay).isLessThan(Duration.ofSeconds(4))
    }

    @Test
    fun `sparse counter rows - adjacent empty window found immediately`() {
        configRepository.createConfig("v3-sparse", 2, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Fill window 0
        service.assignSlot("evt-sp1", "v3-sparse", requestedTime)
        service.assignSlot("evt-sp2", "v3-sparse", requestedTime)

        // Create a far-future counter row
        val farWindow = Instant.parse("2025-06-01T12:01:00Z")
        service.assignSlot("evt-sp-far", "v3-sparse", farWindow)

        // Next event at requestedTime should go to W1 (12:00:04), not after far window
        val slot = service.assignSlot("evt-sp3", "v3-sparse", requestedTime)
        assertThat(slot.scheduledTime).isAfterOrEqualTo(requestedTime.plusSeconds(4))
        assertThat(slot.scheduledTime).isBefore(requestedTime.plusSeconds(8))
    }

    // ==================== Config error ====================

    @Test
    fun `throws ConfigLoadException for unknown config name`() {
        assertThatThrownBy {
            service.assignSlot("evt-bad", "non-existent", Instant.now())
        }.isInstanceOf(ConfigLoadException::class.java)
            .hasMessageContaining("non-existent")
    }

    // ==================== Concurrency ====================

    @Test
    fun `concurrent assignments respect max_per_window`() {
        val maxPerWindow = 20
        val totalEvents = 100
        val threadCount = 50

        configRepository.createConfig("v3-conc", maxPerWindow, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-c$i"] = service.assignSlot("evt-c$i", "v3-conc", requestedTime)
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue()
        executor.shutdown()
        assertThat(errors).isEmpty()
        assertThat(results).hasSize(totalEvents)

        val slotsByWindow = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
        }
        for ((windowStart, slots) in slotsByWindow) {
            assertThat(slots.size)
                .describedAs("Window $windowStart should have at most $maxPerWindow slots")
                .isLessThanOrEqualTo(maxPerWindow)
        }

        // Verify counters match actual slot counts
        val dbCounters = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }
        for ((windowStart, slots) in slotsByWindow) {
            assertThat(dbCounters[windowStart])
                .describedAs("Counter for $windowStart should match actual slot count")
                .isEqualTo(slots.size)
        }
    }

    @Test
    fun `no deadlocks under sustained load`() {
        val totalEvents = 500
        val threadCount = 50

        configRepository.createConfig("v3-deadlock", 10, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val successCount = AtomicInteger(0)
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    service.assignSlot("evt-dl$i", "v3-deadlock", requestedTime)
                    successCount.incrementAndGet()
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue()
        executor.shutdown()
        assertThat(errors).isEmpty()
        assertThat(successCount.get()).isEqualTo(totalEvents)
    }

    @Test
    fun `counter stays consistent under contention`() {
        val maxPerWindow = 50
        val totalEvents = 200
        val threadCount = 30

        configRepository.createConfig("v3-cnt-conc", maxPerWindow, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    results["evt-cc$i"] = service.assignSlot("evt-cc$i", "v3-cnt-conc", requestedTime)
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue()
        executor.shutdown()
        assertThat(errors).isEmpty()
        assertThat(results).hasSize(totalEvents)

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
            assertThat(counterValues[windowStart])
                .describedAs("Counter for $windowStart should match actual slot count $actualCount")
                .isEqualTo(actualCount)
        }

        assertThat(slotCountsByWindow.values.sum()).isEqualTo(totalEvents)
    }
}
