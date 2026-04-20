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

/**
 * Append-only frontier tracker for chunked window provisioning.
 * Multiple rows per REQ_TS — one per chunk extension.
 * Read: SELECT MAX(WNDW_END_TS) WHERE REQ_TS = ?
 * Write: INSERT (REQ_TS, WNDW_END_TS) — duplicates absorbed by composite PK.
 */
object WindowChunkFrontierTable : Table("RL_WNDW_FRONTIER_TRK") {
    val requestedTime = timestamp("REQ_TS")
    val windowEnd = timestamp("WNDW_END_TS")
    val createdAt = timestamp("CREAT_TS")

    override val primaryKey = PrimaryKey(requestedTime, windowEnd)
}

/**
 * Versioned rate-limit config. Multiple rows per WNDW_CONFIG_NM may exist for audit;
 * exactly one should have ACT_IN = 1 at any given time per name.
 */
object RateLimitConfigTable : Table("RL_EVENT_WNDW_CONFIG") {
    val configId = varchar("RL_WNDW_CONFIG_ID", 50)
    val configName = varchar("WNDW_CONFIG_NM", 128)
    val maxPerWindow = integer("WNDW_MAX_EVENT_CT")
    val windowSizeIso = varchar("WNDW_SIZE_ISO_DUR_TX", 25)
    val effectiveFrom = timestamp("CONFIG_EFF_STRT_DT")
    val isActive = integer("ACT_IN")
    val createdAt = timestamp("CREAT_TS")

    override val primaryKey = PrimaryKey(configId)
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
