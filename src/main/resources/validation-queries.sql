-- ============================================================================
-- Validation & Debugging Queries for Rate Limiter
-- ============================================================================
-- Operational queries for inspecting slot distribution, chunk usage, and
-- counter consistency. NOT part of the application runtime.
-- Target: Oracle 19c+
-- ============================================================================


-- ============================================================================
-- 1. Distribution of slots across windows
-- ============================================================================
-- Compares RL_WNDW_CT.SLOT_CT against actual RL_EVENT_SLOT_DTL rows.
-- counter_drift != 0 indicates a consistency bug.

SELECT
    wc.WNDW_STRT_TS,
    wc.SLOT_CT                           AS counter_value,
    NVL(es.actual_count, 0)              AS actual_slot_count,
    wc.SLOT_CT - NVL(es.actual_count, 0) AS counter_drift
FROM RL_WNDW_CT wc
LEFT JOIN (
    SELECT WNDW_STRT_TS, COUNT(*) AS actual_count
    FROM   RL_EVENT_SLOT_DTL
    GROUP BY WNDW_STRT_TS
) es ON es.WNDW_STRT_TS = wc.WNDW_STRT_TS
WHERE wc.SLOT_CT > 0
ORDER BY wc.WNDW_STRT_TS;


-- ============================================================================
-- 2. Number of chunks (frontier rows) for a given REQ_TS
-- ============================================================================
-- Shows all frontier rows and their order. Count indicates how many chunk
-- extensions have occurred.
-- Replace :requested_time with the epoch-aligned window boundary.

SELECT
    REQ_TS,
    WNDW_END_TS,
    ROW_NUMBER() OVER (ORDER BY WNDW_END_TS) AS chunk_number
FROM   RL_WNDW_FRONTIER_TRK
WHERE  REQ_TS = :requested_time
ORDER BY WNDW_END_TS;

-- Count only:
SELECT COUNT(*) AS chunk_count
FROM   RL_WNDW_FRONTIER_TRK
WHERE  REQ_TS = :requested_time;


-- ============================================================================
-- 3. Min, max occupied slots in a chunk
-- ============================================================================
-- For a given chunk range [chunk_start, chunk_end), shows SLOT_CT stats.
-- Replace :chunk_start and :chunk_end with chunk boundaries.

SELECT
    COUNT(*)        AS windows_in_range,
    MIN(SLOT_CT)    AS min_slots,
    MAX(SLOT_CT)    AS max_slots,
    ROUND(AVG(SLOT_CT), 2) AS avg_slots,
    SUM(SLOT_CT)    AS total_slots
FROM RL_WNDW_CT
WHERE WNDW_STRT_TS >= :chunk_start
  AND WNDW_STRT_TS <  :chunk_end;


-- ============================================================================
-- 4. Min, max occupied slots for a given REQ_TS
-- ============================================================================
-- Finds all windows used by events with a specific REQ_TS, then reads
-- their shared counters. Note: counters are shared across REQ_TS values,
-- so counter values may exceed slots attributed to this REQ_TS alone.
-- Replace :requested_time with the original REQ_TS from the API call.

SELECT
    MIN(wc.SLOT_CT)              AS min_slots_in_used_windows,
    MAX(wc.SLOT_CT)              AS max_slots_in_used_windows,
    ROUND(AVG(wc.SLOT_CT), 2)   AS avg_slots_in_used_windows,
    COUNT(DISTINCT es.WNDW_STRT_TS) AS windows_used,
    SUM(es.event_count)          AS total_events_for_requested_time
FROM (
    SELECT WNDW_STRT_TS, COUNT(*) AS event_count
    FROM   RL_EVENT_SLOT_DTL
    WHERE  REQ_TS = :requested_time
    GROUP BY WNDW_STRT_TS
) es
JOIN RL_WNDW_CT wc
  ON wc.WNDW_STRT_TS = es.WNDW_STRT_TS;
