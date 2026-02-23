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
-- Compares window_counter.slot_count against actual event_slot rows.
-- counter_drift != 0 indicates a consistency bug.

SELECT
    wc.window_start,
    wc.slot_count                           AS counter_value,
    NVL(es.actual_count, 0)                 AS actual_slot_count,
    wc.slot_count - NVL(es.actual_count, 0) AS counter_drift
FROM rate_limit_window_counter wc
LEFT JOIN (
    SELECT window_start, COUNT(*) AS actual_count
    FROM   rate_limit_event_slot
    GROUP BY window_start
) es ON es.window_start = wc.window_start
WHERE wc.slot_count > 0
ORDER BY wc.window_start;


-- ============================================================================
-- 2. Number of chunks (frontier rows) for a given requestedTime
-- ============================================================================
-- Shows all frontier rows and their order. Count indicates how many chunk
-- extensions have occurred.
-- Replace :requested_time with the epoch-aligned window boundary.

SELECT
    requested_time,
    window_end,
    ROW_NUMBER() OVER (ORDER BY window_end) AS chunk_number
FROM   track_window_end
WHERE  requested_time = :requested_time
ORDER BY window_end;

-- Count only:
SELECT COUNT(*) AS chunk_count
FROM   track_window_end
WHERE  requested_time = :requested_time;


-- ============================================================================
-- 3. Min, max occupied slots in a chunk
-- ============================================================================
-- For a given chunk range [chunk_start, chunk_end), shows slot_count stats.
-- Replace :chunk_start and :chunk_end with chunk boundaries.

SELECT
    COUNT(*)        AS windows_in_range,
    MIN(slot_count) AS min_slots,
    MAX(slot_count) AS max_slots,
    ROUND(AVG(slot_count), 2) AS avg_slots,
    SUM(slot_count) AS total_slots
FROM rate_limit_window_counter
WHERE window_start >= :chunk_start
  AND window_start <  :chunk_end;


-- ============================================================================
-- 4. Min, max occupied slots for a given requestedTime
-- ============================================================================
-- Finds all windows used by events with a specific requestedTime, then reads
-- their shared counters. Note: counters are shared across requestedTime values,
-- so counter values may exceed slots attributed to this requestedTime alone.
-- Replace :requested_time with the original requested_time from the API call.

SELECT
    MIN(wc.slot_count)          AS min_slots_in_used_windows,
    MAX(wc.slot_count)          AS max_slots_in_used_windows,
    ROUND(AVG(wc.slot_count), 2) AS avg_slots_in_used_windows,
    COUNT(DISTINCT es.window_start) AS windows_used,
    SUM(es.event_count)         AS total_events_for_requested_time
FROM (
    SELECT window_start, COUNT(*) AS event_count
    FROM   rate_limit_event_slot
    WHERE  requested_time = :requested_time
    GROUP BY window_start
) es
JOIN rate_limit_window_counter wc
  ON wc.window_start = es.window_start;
