package com.ratelimiter

import com.ratelimiter.config.RateLimitConfigRepository
import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentService
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.iterator

@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
class ConcurrencyTest {

    @Inject
    lateinit var service: SlotAssignmentService

    @Inject
    lateinit var configRepository: RateLimitConfigRepository

    @BeforeEach
    fun setup() {
        transaction {
            RateLimitEventSlotTable.deleteAll()
            WindowCounterTable.deleteAll()
        }
        configRepository.evictCache()
    }

    @Test
    fun `concurrent assignments respect max_per_window`() {
        val maxPerWindow = 20
        val totalEvents = 100
        val threadCount = 50

        configRepository.createConfig("test-concurrent", maxPerWindow, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    val slot = service.assignSlot("evt-conc-$i", "test-concurrent", requestedTime)
                    results["evt-conc-$i"] = slot
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        val completed = latch.await(60, TimeUnit.SECONDS)
        executor.shutdown()

        assertThat(completed).isTrue()
        assertThat(errors).isEmpty()
        assertThat(results).hasSize(totalEvents)

        // Verify no window exceeds max_per_window
        val slotsByWindow = results.values.groupBy { it.windowStart }
        for ((windowStart, slots) in slotsByWindow) {
            assertThat(slots.size)
                .describedAs("Window $windowStart should have at most $maxPerWindow slots")
                .isLessThanOrEqualTo(maxPerWindow)
        }

        // Verify total slots match
        assertThat(results.values.size).isEqualTo(totalEvents)

        // Verify counter values match actual slot counts per window
        val dbCounters = transaction {
            WindowCounterTable.selectAll().associate { row ->
                row[WindowCounterTable.windowStart] to row[WindowCounterTable.slotCount]
            }
        }

        for ((windowStart, slots) in slotsByWindow) {
            val dbCount = dbCounters[windowStart]
            assertThat(dbCount)
                .describedAs("DB counter for window $windowStart should match actual slot count")
                .isEqualTo(slots.size)
        }

        // Verify expected number of windows used
        val expectedWindows = (totalEvents + maxPerWindow - 1) / maxPerWindow // ceiling division
        assertThat(slotsByWindow.keys.size)
            .describedAs("Should use approximately $expectedWindows windows")
            .isGreaterThanOrEqualTo(expectedWindows)
    }

    @Test
    fun `idempotent concurrent calls return same slot`() {
        configRepository.createConfig("test-idem-conc", 100, 4)
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
                    val slot = service.assignSlot(eventId, "test-idem-conc", requestedTime)
                    results.add(slot)
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        val completed = latch.await(30, TimeUnit.SECONDS)
        executor.shutdown()

        assertThat(completed).isTrue()
        assertThat(errors).isEmpty()
        assertThat(results).hasSize(threadCount)

        // All results should be identical
        val uniqueSlotIds = results.map { it.slotId }.distinct()
        assertThat(uniqueSlotIds).hasSize(1)

        val uniqueScheduledTimes = results.map { it.scheduledTime }.distinct()
        assertThat(uniqueScheduledTimes).hasSize(1)

        // Only one row should exist in DB
        val dbCount = transaction {
            RateLimitEventSlotTable.selectAll()
                .where { RateLimitEventSlotTable.eventId eq eventId }
                .count()
        }
        assertThat(dbCount).isEqualTo(1)
    }

    @Test
    fun `counter stays consistent under contention`() {
        val maxPerWindow = 50
        val totalEvents = 200
        val threadCount = 30

        configRepository.createConfig("test-counter-conc", maxPerWindow, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val results = ConcurrentHashMap<String, AssignedSlot>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    val slot = service.assignSlot("evt-cnt-conc-$i", "test-counter-conc", requestedTime)
                    results["evt-cnt-conc-$i"] = slot
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        val completed = latch.await(60, TimeUnit.SECONDS)
        executor.shutdown()

        assertThat(completed).isTrue()
        assertThat(errors).isEmpty()
        assertThat(results).hasSize(totalEvents)

        // For each window, verify counter == actual number of rate_limit_event_slot rows
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
            val counterValue = counterValues[windowStart]
            assertThat(counterValue)
                .describedAs("Counter for window $windowStart should match actual slot count $actualCount")
                .isEqualTo(actualCount)
        }

        // Total slots in DB should match events submitted
        val totalSlots = slotCountsByWindow.values.sum()
        assertThat(totalSlots).isEqualTo(totalEvents)
    }

    @Test
    fun `no deadlocks under sustained load`() {
        val totalEvents = 500
        val threadCount = 50

        configRepository.createConfig("test-deadlock", 10, 4)
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val successCount = AtomicInteger(0)
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latch = CountDownLatch(totalEvents)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                try {
                    service.assignSlot("evt-dl-$i", "test-deadlock", requestedTime)
                    successCount.incrementAndGet()
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latch.countDown()
                }
            }
        }

        val completed = latch.await(120, TimeUnit.SECONDS)
        executor.shutdown()

        assertThat(completed).isTrue()

        // All events should succeed (no SlotAssignmentException since headroom is 100 windows)
        // 500 events / 10 per window = 50 windows needed, well within 100 headroom
        assertThat(errors).isEmpty()
        assertThat(successCount.get()).isEqualTo(totalEvents)
    }
}
