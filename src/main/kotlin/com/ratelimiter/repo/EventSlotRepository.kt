package com.ratelimiter.repo

import com.ratelimiter.db.RateLimitEventSlotTable
import com.ratelimiter.slot.AssignedSlot
import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
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
            } catch (_: ExposedSQLException) {
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
        } catch (_: ExposedSQLException) {
            // Unique constraint violation on event_id — slot already exists
            false
        }
    }
}
