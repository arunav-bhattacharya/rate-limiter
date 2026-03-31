package com.ratelimiter.db

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp

/** Per-window slot counter — config-agnostic concurrency control. */
object WindowCounterTable : Table("RL_WNDW_CT") {
    val windowStart = timestamp("WNDW_STRT_TS")
    val slotCount = integer("SLOT_CT").default(0)
    val windowStatus = varchar("WNDW_STATUS", 10).default("AVAILABLE")
    val createdAt = timestamp("CREAT_TS")

    override val primaryKey = PrimaryKey(windowStart)

    init {
        index("RL_WNDW_CT_I01X", false, windowStart, slotCount)
        index("RL_WNDW_CT_I02X", false, windowStatus, windowStart)
    }
}

/**
 * Append-only skip pointer for V5 multi-pod coordination.
 * Composite PK (REQ_TS, SKIP_TO_TS) — multiple rows per requestedTime.
 * Read: SELECT SKIP_TO_TS WHERE REQ_TS = ? ORDER BY SKIP_TO_TS DESC FETCH FIRST 1 ROW ONLY
 * Write: INSERT (rt, skipTo) — duplicates silently caught by PK.
 */
object SkipPointerTable : Table("RL_SKIP_PTR") {
    val requestedTime = timestamp("REQ_TS")
    val skipTo = timestamp("SKIP_TO_TS")
    val createdAt = timestamp("CREAT_TS")

    override val primaryKey = PrimaryKey(requestedTime, skipTo)
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
