package com.payments.ratelimiter.db

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp

/** Rate limit configuration — dynamic, versioned. */
object RateLimitConfigTable : Table("rate_limit_config") {
    val id = long("id").autoIncrement()
    val configName = varchar("config_name", 128)
    val maxPerWindow = integer("max_per_window")
    val windowSizeSecs = integer("window_size_secs")
    val effectiveFrom = timestamp("effective_from")
    val isActive = bool("is_active")
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(id)
}

/** Per-window slot counter — config-agnostic concurrency control. */
object WindowCounterTable : Table("rate_limit_window_counter") {
    val windowStart = timestamp("window_start")
    val slotCount = integer("slot_count")

    override val primaryKey = PrimaryKey(windowStart)
}

/** Immutable slot assignment record. */
object ScheduledEventSlotTable : Table("scheduled_event_slot") {
    val id = long("id").autoIncrement()
    val eventId = varchar("event_id", 256)
    val requestedTime = timestamp("requested_time")
    val windowStart = timestamp("window_start")
    val slotIndex = integer("slot_index")
    val scheduledTime = timestamp("scheduled_time")
    val configId = long("config_id").references(RateLimitConfigTable.id)
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(id)
}
