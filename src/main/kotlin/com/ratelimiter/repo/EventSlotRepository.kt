package com.ratelimiter.repo

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.db.isDuplicateKeyViolation
import com.ratelimiter.slot.AssignedSlot
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.count
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.SortOrder.DESC
import org.jetbrains.exposed.sql.and
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@ApplicationScoped
class EventSlotRepository {

    /**
     * Idempotency check — look up an existing slot by eventId.
     * Runs in its own transaction (called before the main assignment transaction).
     */
    fun fetchAssignedSlot(eventId: String): AssignedSlot? {
        return transaction {
            queryAssignedSlot(eventId)
        }
    }

    /**
     * Query an existing slot within the current transaction.
     * Used in the duplicate-key recovery path inside claimSlot.
     */
    fun Transaction.queryAssignedSlot(eventId: String): AssignedSlot? {
        return RateLimitEventSlotTable
            .selectAll()
            .where { RateLimitEventSlotTable.eventId eq eventId }
            .firstOrNull()
            ?.let { row ->
                val requestedTime = row[RateLimitEventSlotTable.requestedTime]
                val scheduledTime = row[RateLimitEventSlotTable.scheduledTime]
                val delay = Duration.between(requestedTime, scheduledTime).let { d ->
                    if (d.isNegative) Duration.ZERO else d
                }
                AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
            }
    }

    /**
     * Insert a new event slot row, managing its own transaction.
     * Used by the REST endpoint (EventSlotResource) for direct inserts.
     */
    fun insertEventSlotInNewTransaction(
        eventId: String,
        requestedTime: Instant,
        windowStart: Instant,
        scheduledTime: Instant,
        configId: String
    ): Boolean {
        return transaction {
            val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            try {
                RateLimitEventSlotTable.insert {
                    it[slotId] = UUID.randomUUID().toString()
                    it[RateLimitEventSlotTable.eventId] = eventId
                    it[RateLimitEventSlotTable.requestedTime] = requestedTime
                    it[RateLimitEventSlotTable.windowStart] = windowStart
                    it[RateLimitEventSlotTable.scheduledTime] = scheduledTime
                    it[RateLimitEventSlotTable.configId] = configId
                    it[RateLimitEventSlotTable.createdAt] = now
                }
                true
            } catch (e: ExposedSQLException) {
                if (!e.isDuplicateKeyViolation()) throw e
                false
            }
        }
    }

    /**
     * Insert a new event slot row within an existing transaction.
     * Returns true if the row was inserted, false if a duplicate eventId already exists.
     */
    fun Transaction.insertEventSlot(
        eventId: String,
        requestedTime: Instant,
        windowStart: Instant,
        scheduledTime: Instant,
        configId: String
    ): Boolean {
        val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        return try {
            RateLimitEventSlotTable.insert {
                it[slotId] = UUID.randomUUID().toString()
                it[RateLimitEventSlotTable.eventId] = eventId
                it[RateLimitEventSlotTable.requestedTime] = requestedTime
                it[RateLimitEventSlotTable.windowStart] = windowStart
                it[RateLimitEventSlotTable.scheduledTime] = scheduledTime
                it[RateLimitEventSlotTable.configId] = configId
                it[RateLimitEventSlotTable.createdAt] = now
            }
            true
        } catch (e: ExposedSQLException) {
            if (!e.isDuplicateKeyViolation()) throw e
            false
        }
    }

    /**
     * Returns the furthest window that has actual slot assignments for a given requestedTime.
     * Used by V3 and V4 to compute the scan upper bound (maxUsed + headroom).
     */
    fun fetchMaxWindowStartTime(requestedTime: Instant): Instant? {
        return transaction {
            RateLimitEventSlotTable
                .select(RateLimitEventSlotTable.windowStart)
                .where { RateLimitEventSlotTable.requestedTime eq requestedTime }
                .orderBy(RateLimitEventSlotTable.windowStart, DESC)
                .limit(1)
                .firstOrNull()
                ?.get(RateLimitEventSlotTable.windowStart)
        }
    }

    // ==================== V4 methods ====================

    /**
     * Full windows (slot count >= [softMax]) in [rangeStart, rangeEnd).
     * Counts globally across all requestedTimes (shared window capacity).
     * Narrow index range scan + GROUP BY — bounded by the adaptive scan range.
     */
    fun findFullWindowsInRange(rangeStart: Instant, rangeEnd: Instant, softMax: Int): Set<Instant> {
        val slotCount = RateLimitEventSlotTable.slotId.count()
        return transaction {
            RateLimitEventSlotTable
                .select(RateLimitEventSlotTable.windowStart)
                .where {
                    (RateLimitEventSlotTable.windowStart greaterEq rangeStart) and
                            (RateLimitEventSlotTable.windowStart less rangeEnd)
                }
                .groupBy(RateLimitEventSlotTable.windowStart)
                .having { slotCount greaterEq softMax.toLong() }
                .mapTo(mutableSetOf()) { it[RateLimitEventSlotTable.windowStart] }
        }
    }

    /**
     * Insert a slot and return AssignedSlot.
     * On duplicate EVENT_ID, returns the existing slot (idempotent).
     * Returns null only if the insert fails for a non-duplicate reason (shouldn't happen).
     */
    fun insertAndReturnSlot(
        eventId: String,
        windowStart: Instant,
        scheduledTime: Instant,
        configId: String,
        requestedTime: Instant
    ): AssignedSlot {
        return transaction {
            val inserted = insertEventSlot(eventId, requestedTime, windowStart, scheduledTime, configId)

            if (!inserted) {
                return@transaction queryAssignedSlot(eventId)
                    ?: error("Failed to re-read slot for eventId=$eventId after duplicate key")
            }

            val delay = Duration.between(requestedTime, scheduledTime).let { d ->
                if (d.isNegative) Duration.ZERO else d
            }
            AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
        }
    }
}
