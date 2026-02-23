package com.ratelimiter.slot

/**
 * SQL constants for the slot assignment algorithm.
 */
internal object SlotAssignmentSql {

    /** Status codes returned by the PL/SQL block via the ou_status OUT parameter. */
    const val STATUS_NEW = 1
    const val STATUS_EXISTING = 0
    const val STATUS_EXHAUSTED = -1

    /**
     * Anonymous PL/SQL block that performs the entire slot assignment in a single
     * database round trip. Contains four local functions:
     *
     * - check_existing_slot: Idempotency pre-check (SELECT by event_id)
     * - try_lock_window: Ensure counter row + SELECT FOR UPDATE SKIP LOCKED
     * - claim_slot_in_window: INSERT slot + UPDATE counter, with ORA-00001 handling
     * - find_first_available_window: Adjacent-first + index scan to find first non-full window
     *
     * The main block uses a two-phase approach:
     *   Phase 1: Try the first (floor-aligned) window with pre-computed proportional capacity
     *   Phase 2: Chunked adaptive search — skip to first available window, walk within
     *            headroom, re-skip between chunks up to max_search_chunks times
     *
     * Jitter and proportional capacity are pre-computed in Kotlin and passed as IN params.
     *
     * Parameter positions:
     *   IN:  1=event_id, 2=window_start, 3=requested_time, 4=config_id,
     *        5=max_per_window, 6=window_size_secs, 7=max_first_window,
     *        8=first_jitter_ms, 9=full_jitter_ms, 10=headroom_secs,
     *        11=max_search_chunks
     *   OUT: 12=status, 13=slot_id, 14=scheduled_time, 15=window_start_out,
     *        16=windows_searched
     *
     * Loaded from: assign-slot.sql
     */
    val ASSIGN_SLOT_PLSQL = SlotAssignmentSql::class.java.getResourceAsStream("/assign-slot.sql")
        ?.bufferedReader()
        ?.use { it.readText() }
        ?: throw IllegalStateException("Failed to load assign-slot.sql from resources")
}
