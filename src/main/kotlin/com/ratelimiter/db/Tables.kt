package com.ratelimiter.db

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp

/** Rate limit configuration — dynamic, versioned. */
object RateLimitConfigTable : Table("rate_limit_config") {
    val configId = long("config_id").autoIncrement()
    val configName = varchar("config_name", 128)
    val maxPerWindow = integer("max_per_window")
    val windowSize = varchar("window_size", 50)
    val headroomWindows = integer("headroom_windows").nullable()
    val effectiveFrom = timestamp("effective_from")
    val isActive = bool("is_active")
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(configId)
}

/** Per-window slot counter — config-agnostic concurrency control. */
object WindowCounterTable : Table("rate_limit_window_counter") {
    val windowStart = timestamp("window_start")
    val slotCount = integer("slot_count")
    val status = varchar("status", 10)

    override val primaryKey = PrimaryKey(windowStart)
}

/** Shared on-demand horizon state for materializing window rows. */
object HorizonStateTable : Table("rate_limit_horizon_state") {
    val horizonKey = varchar("horizon_key", 32)
    val horizonStart = timestamp("horizon_start")
    val horizonEnd = timestamp("horizon_end")
    val windowSizeSecs = integer("window_size_secs").nullable()
    val updatedAt = timestamp("updated_at")

    override val primaryKey = PrimaryKey(horizonKey)
}

/** Immutable slot assignment record. */
object RateLimitEventSlotTable : Table("rate_limit_event_slot") {
    val slotId = long("slot_id").autoIncrement()
    val eventId = varchar("event_id", 256)
    val requestedTime = timestamp("requested_time")
    val windowStart = timestamp("window_start")
    val scheduledTime = timestamp("scheduled_time")
    val configId = long("config_id")
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(slotId)
}
