package com.ratelimiter

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.WindowCounterTable
import com.ratelimiter.db.WindowEndTrackerTable
import com.ratelimiter.repo.EventSlotRepository
import com.ratelimiter.repo.RateLimitConfigRepository
import com.ratelimiter.repo.WindowEndTrackerRepository
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

@QuarkusTest
@QuarkusTestResource(OracleTestResource::class)
@Tag("perf")
class SlotAssignmentServiceV3PerfTest {

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

    // ==================== Perf: Concurrent burst ====================

    @Test
    fun `perf - 1000 concurrent events at same requestedTime`() {
        val totalEvents = 1000
        val threadCount = 50
        val maxPerWindow = 100

        configRepository.createConfig("perf-burst", maxPerWindow, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

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
                    val slot = service.assignSlot("evt-perf-$i", "perf-burst", requestedTime)
                    results["evt-perf-$i"] = slot
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

        // Correctness
        assertTrue(errors.isEmpty())
        assertEquals(totalEvents, results.size)

        val slotsByWindow = transaction {
            RateLimitEventSlotTable.selectAll().toList()
                .groupBy { it[RateLimitEventSlotTable.windowStart] }
        }
        for ((ws, slots) in slotsByWindow) {
            assertTrue(
                slots.size <= maxPerWindow,
                "Window $ws should have at most $maxPerWindow slots"
            )
        }

        // Report
        println("  Windows used: ${slotsByWindow.size}")
        reportLatency("1000 concurrent events (50 threads, maxPerWindow=$maxPerWindow)", latencies, totalNs, totalEvents)
    }

    // ==================== Perf: Fill ratio impact ====================

    @Test
    fun `perf - latency at increasing fill ratios`() {
        val maxPerWindow = 10
        val initialChunkCapacity = maxPerWindow * 100 // 100 = default maxWindowsInChunk
        configRepository.createConfig("perf-fill", maxPerWindow, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        val fillPercents = listOf(0, 25, 50, 75, 90)
        var eventCounter = 0

        for (fillPercent in fillPercents) {
            val targetTotal = (fillPercent.toLong() * initialChunkCapacity / 100).toInt()

            // Pre-fill to target
            while (eventCounter < targetTotal) {
                eventCounter++
                service.assignSlot("evt-fill-$eventCounter", "perf-fill", requestedTime)
            }

            // Measure a batch of 50 events from 10 threads
            val batchSize = 50
            val threadCount = 10
            val batchIds = (1..batchSize).map { j -> "evt-fill-${eventCounter + j}" }
            eventCounter += batchSize

            val batchLatencies = CopyOnWriteArrayList<Long>()
            val batchErrors = ConcurrentLinkedQueue<Throwable>()
            val batchLatch = CountDownLatch(batchSize)
            val batchStartGate = CountDownLatch(1)
            val batchExecutor = Executors.newFixedThreadPool(threadCount)

            repeat(batchSize) { j ->
                batchExecutor.submit {
                    batchStartGate.await()
                    val s = System.nanoTime()
                    try {
                        service.assignSlot(batchIds[j], "perf-fill", requestedTime)
                    } catch (e: Throwable) {
                        batchErrors.add(e)
                    } finally {
                        batchLatencies.add(System.nanoTime() - s)
                        batchLatch.countDown()
                    }
                }
            }

            val batchStart = System.nanoTime()
            batchStartGate.countDown()
            assertTrue(batchLatch.await(60, TimeUnit.SECONDS))
            val batchTotal = System.nanoTime() - batchStart
            batchExecutor.shutdown()

            assertTrue(batchErrors.isEmpty())
            reportLatency("Fill ratio ~${fillPercent}% ($targetTotal pre-filled of $initialChunkCapacity)", batchLatencies, batchTotal, batchSize)
        }
    }

    // ==================== Perf: Chunk extension ====================

    @Test
    fun `perf - chunk extension overhead with maxPerWindow=1`() {
        configRepository.createConfig("perf-chunk", 1, Duration.ofSeconds(4))
        val requestedTime = Instant.parse("2025-06-01T12:00:00Z")

        // Phase 1: First window (1 event)
        val phase1Start = System.nanoTime()
        service.assignSlot("evt-chunk-1", "perf-chunk", requestedTime)
        val phase1Ns = System.nanoTime() - phase1Start

        // Phase 2: Initial chunk (events 2..101 = 100 events, sequential)
        val phase2Latencies = mutableListOf<Long>()
        for (i in 2..101) {
            val s = System.nanoTime()
            service.assignSlot("evt-chunk-$i", "perf-chunk", requestedTime)
            phase2Latencies.add(System.nanoTime() - s)
        }

        // Phase 3: Extension chunk (events 102..201 = 100 events, sequential)
        val phase3Latencies = mutableListOf<Long>()
        for (i in 102..201) {
            val s = System.nanoTime()
            service.assignSlot("evt-chunk-$i", "perf-chunk", requestedTime)
            phase3Latencies.add(System.nanoTime() - s)
        }

        // Correctness
        val totalSlots = transaction {
            RateLimitEventSlotTable.selectAll().count()
        }
        assertEquals(201L, totalSlots)

        val frontierRows = transaction {
            WindowEndTrackerTable.selectAll()
                .where { WindowEndTrackerTable.requestedTime eq requestedTime }
                .count()
        }
        assertTrue(frontierRows >= 2)

        // Report
        val p2Sorted = phase2Latencies.sorted()
        val p3Sorted = phase3Latencies.sorted()
        println("""
            |=== Chunk Extension Overhead (maxPerWindow=1) ===
            |  Phase 1 (first window, 1 event):     ${phase1Ns / 1_000_000}ms
            |  Phase 2 (initial chunk, 100 events):
            |    Avg:   ${"%.2f".format(phase2Latencies.average() / 1_000_000)}ms
            |    p50:   ${p2Sorted[(p2Sorted.size * 0.50).toInt()] / 1_000_000}ms
            |    p99:   ${p2Sorted[(p2Sorted.size * 0.99).toInt()] / 1_000_000}ms
            |    First (triggers provisioning): ${phase2Latencies.first() / 1_000_000}ms
            |    Last:  ${phase2Latencies.last() / 1_000_000}ms
            |  Phase 3 (extension chunk, 100 events):
            |    Avg:   ${"%.2f".format(phase3Latencies.average() / 1_000_000)}ms
            |    p50:   ${p3Sorted[(p3Sorted.size * 0.50).toInt()] / 1_000_000}ms
            |    p99:   ${p3Sorted[(p3Sorted.size * 0.99).toInt()] / 1_000_000}ms
            |    First (triggers provisioning): ${phase3Latencies.first() / 1_000_000}ms
            |    Last:  ${phase3Latencies.last() / 1_000_000}ms
            |  Frontier rows: $frontierRows
        """.trimMargin())
    }

    // ==================== Helper ====================

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
