package com.ratelimiter.db

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp

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
        index("RL_EVENT_SLOT_DTL_I03X", false, requestedTime, windowStart)
    }
}
