package com.ratelimiter.db

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp

/** Rate limit configuration — dynamic, versioned. */
object RateLimitConfigTable : Table("rate_limit_config") {
    val configId = long("config_id").autoIncrement()
    val configName = varchar("config_name", 128)
    val maxPerWindow = integer("max_per_window")
    val windowSize = varchar("window_size", 50)
    val effectiveFrom = timestamp("effective_from")
    val isActive = bool("is_active")
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(configId)

    init {
        index("idx_config_name_active", false, configName, isActive)
    }
}

/** Per-window slot counter — config-agnostic concurrency control. */
object WindowCounterTable : Table("rate_limit_window_counter") {
    val windowStart = timestamp("window_start")
    val slotCount = integer("slot_count").default(0)

    override val primaryKey = PrimaryKey(windowStart)

    init {
        index("idx_window_counter_slot_start", false, slotCount, windowStart)
    }
}

/** Immutable slot assignment record. */
object RateLimitEventSlotTable : Table("rate_limit_event_slot") {
    val slotId = long("slot_id").autoIncrement()
    val eventId = varchar("event_id", 256).uniqueIndex("uq_event_id")
    val requestedTime = timestamp("requested_time")
    val windowStart = timestamp("window_start")
    val scheduledTime = timestamp("scheduled_time")
    val configId = long("config_id")
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(slotId)

    init {
        index("idx_event_slot_window", false, windowStart)
        index("idx_event_slot_created", false, createdAt)
    }
}

/** Append-only provisioning frontier tracker. Composite PK = (requested_time, window_end). */
object WindowEndTrackerTable : Table("track_window_end") {
    val requestedTime = timestamp("requested_time")
    val windowEnd = timestamp("window_end")

    override val primaryKey = PrimaryKey(requestedTime, windowEnd)
}
