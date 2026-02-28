package com.ratelimiter.db

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp

/** Rate limit configuration — dynamic, versioned. */
object RateLimitConfigTable : Table("RL_EVENT_WNDW_CONFIG") {
    val configId = varchar("RL_WNDW_CONFIG_ID", 50)
    val configName = varchar("WNDW_CONFIG_NM", 128)
    val maxPerWindow = integer("WNDW_MAX_EVENT_CT")
    val windowSize = varchar("WNDW_SIZE_ISO_DUR_TX", 25)
    val effectiveFrom = timestamp("CONFIG_EFF_STRT_DT")
    val isActive = bool("ACT_IN")
    val createdAt = timestamp("CREAT_TS")

    override val primaryKey = PrimaryKey(configId)

    init {
        index("RL_EVENT_WNDW_CONFIG_I01X", false, configName, isActive)
    }
}

/** Per-window slot counter — config-agnostic concurrency control. */
object WindowCounterTable : Table("RL_WNDW_CT") {
    val windowStart = timestamp("WNDW_STRT_TS")
    val slotCount = integer("SLOT_CT").default(0)
    val createdAt = timestamp("CREAT_TS")

    override val primaryKey = PrimaryKey(windowStart)

    init {
        index("RL_WNDW_CT_I01X", false, windowStart, slotCount)
    }
}

/** Immutable slot assignment record. */
object RateLimitEventSlotTable : Table("RL_EVENT_SLOT_DTL") {
    val slotId = varchar("WNDW_SLOT_ID", 50)
    val eventId = varchar("EVENT_ID", 50).uniqueIndex("RL_EVENT_SLOT_DTL_IUX")
    val requestedTime = timestamp("REQ_TS")
    val configId = varchar("RL_WNDW_CONFIG_ID", 50)
    val windowStart = timestamp("WNDW_STRT_TS")
    val scheduledTime = timestamp("COMPUTED_SCHED_TS")
    val createdAt = timestamp("CREAT_TS")

    override val primaryKey = PrimaryKey(slotId)

    init {
        index("RL_EVENT_SLOT_DTL_I01X", false, windowStart)
        index("RL_EVENT_SLOT_DTL_I02X", false, createdAt)
    }
}

/** Append-only provisioning frontier tracker. Composite PK = (REQ_TS, WNDW_END_TS). */
object WindowEndTrackerTable : Table("RL_WNDW_FRONTIER_TRK") {
    val requestedTime = timestamp("REQ_TS")
    val windowEnd = timestamp("WNDW_END_TS")
    val createdAt = timestamp("CREAT_TS")

    override val primaryKey = PrimaryKey(requestedTime, windowEnd)
}
