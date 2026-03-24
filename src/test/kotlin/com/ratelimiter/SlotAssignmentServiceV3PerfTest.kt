package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.WindowSlotCounterRepository
import com.ratelimiter.slot.SlotAssignmentServiceV3
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Perf tests for V3 slot assignment with pre-provisioned windows.
 * Test profile: windowSize=4s, maxPerWindow=2.
 */
@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
@Tag("perf")
class SlotAssignmentServiceV3PerfTest {

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

    @Test
    fun `perf - 500 concurrent events at same requestedTime`() {
        val totalEvents = 500
        val threadCount = 50
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        // maxPerWindow=2, 500 events → 250 windows needed
        preProvisionWindows(requestedTime, 500)

        val results = ConcurrentHashMap<String, Any>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val latencies = CopyOnWriteArrayList<Long>()
        val latch = CountDownLatch(totalEvents)
        val startGate = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(totalEvents) { i ->
            executor.submit {
                startGate.await()
                val start = System.nanoTime()
                try {
                    results["evt-perf-$i"] = service.assignSlot("evt-perf-$i", requestedTime)
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    latencies.add(System.nanoTime() - start)
                    latch.countDown()
                }
            }
        }

        val totalStart = System.nanoTime()
        startGate.countDown()
        assertTrue(latch.await(120, TimeUnit.SECONDS))
        val totalNs = System.nanoTime() - totalStart
        executor.shutdown()

        assertTrue(errors.isEmpty())
        assertEquals(totalEvents, results.size)

        val slotsByWindow = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
        }
        for ((ws, slots) in slotsByWindow) {
            assertTrue(slots.size <= 2, "Window $ws should have at most 2 slots")
        }

        println("  Windows used: ${slotsByWindow.size}")
        reportLatency("500 concurrent events (50 threads, pre-provisioned)", latencies, totalNs, totalEvents)
    }

    @Test
    fun `perf - sequential assignment with maxPerWindow=2`() {
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")
        preProvisionWindows(requestedTime, 500)

        val latencies = mutableListOf<Long>()
        for (i in 1..200) {
            val s = System.nanoTime()
            service.assignSlot("evt-seq-$i", requestedTime)
            latencies.add(System.nanoTime() - s)
        }

        val totalSlots = transaction {
            RateLimitEventSlotTable.selectAll().count()
        }
        assertEquals(200L, totalSlots)

        val sorted = latencies.sorted()
        println("""
            |=== Sequential Assignment (maxPerWindow=2, pre-provisioned) ===
            |  Events: 200
            |  Avg:   ${"%.2f".format(latencies.average() / 1_000_000)}ms
            |  p50:   ${sorted[(sorted.size * 0.50).toInt()] / 1_000_000}ms
            |  p99:   ${sorted[(sorted.size * 0.99).toInt()] / 1_000_000}ms
            |  First: ${latencies.first() / 1_000_000}ms
            |  Last:  ${latencies.last() / 1_000_000}ms
        """.trimMargin())
    }

    private fun reportLatency(testName: String, latenciesNs: List<Long>, totalNs: Long, eventCount: Int) {
        val sorted = latenciesNs.sorted()
        val p50 = sorted[(sorted.size * 0.50).toInt()]
        val p95 = sorted[(sorted.size * 0.95).toInt()]
        val p99 = sorted[(sorted.size * 0.99).toInt()]
        val avgMs = sorted.average() / 1_000_000.0
        val throughput = eventCount.toDouble() / (totalNs / 1_000_000_000.0)

        println("""
            |=== $testName ===
            |  Events:      $eventCount
            |  Total time:  ${totalNs / 1_000_000}ms
            |  Throughput:  ${"%.1f".format(throughput)} events/sec
            |  Avg:         ${"%.2f".format(avgMs)}ms
            |  p50:         ${p50 / 1_000_000}ms
            |  p95:         ${p95 / 1_000_000}ms
            |  p99:         ${p99 / 1_000_000}ms
            |  Min:         ${sorted.first() / 1_000_000}ms
            |  Max:         ${sorted.last() / 1_000_000}ms
        """.trimMargin())
    }
}
