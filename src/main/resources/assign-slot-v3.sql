DECLARE
    -- IN bind variables
    in_event_id            VARCHAR2(256) := ?;  /* 1  */
    in_window_start        TIMESTAMP     := ?;  /* 2  */
    in_requested_time      TIMESTAMP     := ?;  /* 3  */
    in_config_id           NUMBER        := ?;  /* 4  */
    in_max_per_window      NUMBER        := ?;  /* 5  */
    in_window_size_secs    NUMBER        := ?;  /* 6  */
    in_max_first_window    NUMBER        := ?;  /* 7  */
    in_first_jitter_ms     NUMBER        := ?;  /* 8  */
    in_full_jitter_ms      NUMBER        := ?;  /* 9  */
    in_max_windows_in_chunk NUMBER       := ?;  /* 10 */
    in_max_chunks_to_search NUMBER       := ?;  /* 11 */

    -- Output result locals (marshalled to OUT bind vars at end)
    ou_status           NUMBER := -1;  -- default: EXHAUSTED
    ou_slot_id          NUMBER;
    ou_scheduled_time   TIMESTAMP;
    ou_window_start     TIMESTAMP;
    ou_windows_searched NUMBER := 0;

    -- Working state
    window_size      INTERVAL DAY TO SECOND := NUMTODSINTERVAL(in_window_size_secs, 'SECOND');
    chunk_size       INTERVAL DAY TO SECOND := NUMTODSINTERVAL(in_max_windows_in_chunk * in_window_size_secs, 'SECOND');
    windows_searched NUMBER := 0;
    slot_claimed     BOOLEAN := FALSE;
    current_count    NUMBER;
    found_window     TIMESTAMP;
    search_from      TIMESTAMP;
    chunk_end        TIMESTAMP;
    v_window_end     TIMESTAMP;

    STATUS_NEW       CONSTANT NUMBER := 1;
    STATUS_EXISTING  CONSTANT NUMBER := 0;
    STATUS_EXHAUSTED CONSTANT NUMBER := -1;

    ---------------------------------------------------------------
    -- check_existing_slot: Idempotency pre-check.
    -- Returns TRUE if event already has a slot (populates ou_ locals).
    ---------------------------------------------------------------

    FUNCTION check_existing_slot RETURN BOOLEAN IS
    BEGIN
        SELECT  slot_id, scheduled_time, window_start
        INTO    ou_slot_id, ou_scheduled_time, ou_window_start
        FROM    rate_limit_event_slot
        WHERE   event_id = in_event_id;
        ou_status           := STATUS_EXISTING;
        ou_windows_searched := 0;
        RETURN TRUE;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN FALSE;
    END check_existing_slot;

    ---------------------------------------------------------------
    -- ensure_window_exists: Create a single counter row if missing.
    ---------------------------------------------------------------

    PROCEDURE ensure_window_exists(window_ts IN TIMESTAMP) IS
    BEGIN
        INSERT INTO rate_limit_window_counter(window_start, slot_count)
        VALUES (window_ts, 0);
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN NULL;
    END ensure_window_exists;

    ---------------------------------------------------------------
    -- ensure_chunk_provisioned: Batch-create counter rows for a
    -- chunk of windows. Guarded by an existence check on the last
    -- window to skip re-provisioning after the first thread.
    ---------------------------------------------------------------

    PROCEDURE ensure_chunk_provisioned(from_ts IN TIMESTAMP) IS
        last_window TIMESTAMP := from_ts + (in_max_windows_in_chunk - 1) * window_size;
        dummy       NUMBER;
    BEGIN
        -- Quick check: if last window exists, chunk is already provisioned
        SELECT 1 INTO dummy
        FROM   rate_limit_window_counter
        WHERE  window_start = last_window;
        RETURN;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            FOR i IN 0..in_max_windows_in_chunk - 1 LOOP
                BEGIN
                    INSERT INTO rate_limit_window_counter(window_start, slot_count)
                    VALUES (from_ts + i * window_size, 0);
                EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN NULL;
                END;
            END LOOP;
    END ensure_chunk_provisioned;

    ---------------------------------------------------------------
    -- try_lock_first_window: Lock the first window's counter row.
    -- Returns slot_count on success, -1 if skipped (locked by
    -- another session via SKIP LOCKED).
    ---------------------------------------------------------------

    FUNCTION try_lock_first_window(window_ts IN TIMESTAMP) RETURN NUMBER IS
        locked_count NUMBER;
    BEGIN
        SELECT slot_count INTO locked_count
        FROM   rate_limit_window_counter
        WHERE  window_start = window_ts
        FOR UPDATE SKIP LOCKED;
        RETURN locked_count;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN -1;
    END try_lock_first_window;

    ---------------------------------------------------------------
    -- find_and_lock: Combined find + lock. Returns the earliest
    -- non-full, non-contended window in the range, or NULL.
    ---------------------------------------------------------------

    FUNCTION find_and_lock(from_ts IN TIMESTAMP, to_ts IN TIMESTAMP) RETURN TIMESTAMP IS
        found TIMESTAMP;
    BEGIN
        SELECT window_start INTO found
        FROM   rate_limit_window_counter
        WHERE  window_start >= from_ts
        AND    window_start < to_ts
        AND    slot_count < in_max_per_window
        ORDER BY window_start
        FETCH FIRST 1 ROW ONLY
        FOR UPDATE SKIP LOCKED;
        RETURN found;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN NULL;
    END find_and_lock;

    ---------------------------------------------------------------
    -- fetch_or_init_window_end: Get (or initialize) the provisioning
    -- frontier for this alignedStart from the append-only
    -- track_window_end table.
    ---------------------------------------------------------------

    FUNCTION fetch_or_init_window_end RETURN TIMESTAMP IS
        v_end TIMESTAMP;
    BEGIN
        SELECT MAX(window_end) INTO v_end
        FROM   track_window_end
        WHERE  requested_time = in_window_start;

        IF v_end IS NOT NULL THEN
            RETURN v_end;
        END IF;

        -- First request for this alignedStart — provision initial chunk
        v_end := in_window_start + window_size + chunk_size;
        ensure_chunk_provisioned(in_window_start + window_size);

        BEGIN
            INSERT INTO track_window_end(requested_time, window_end)
            VALUES (in_window_start, v_end);
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN
                -- Another thread beat us — re-read the max
                SELECT MAX(window_end) INTO v_end
                FROM   track_window_end
                WHERE  requested_time = in_window_start;
        END;

        RETURN v_end;
    END fetch_or_init_window_end;

    ---------------------------------------------------------------
    -- claim_slot: Insert event slot + increment counter.
    -- Handles DUP_VAL_ON_INDEX on event_id for idempotency.
    ---------------------------------------------------------------

    FUNCTION claim_slot(
        window_ts   IN TIMESTAMP,
        p_jitter_ms IN NUMBER
    ) RETURN BOOLEAN IS
        jitter     INTERVAL DAY TO SECOND := NUMTODSINTERVAL(p_jitter_ms / 1000, 'SECOND');
        sched_time TIMESTAMP;
    BEGIN
        sched_time := window_ts + jitter;

        INSERT INTO rate_limit_event_slot(
            event_id, requested_time, window_start,
            scheduled_time, config_id, created_at
        ) VALUES (
            in_event_id, in_requested_time, window_ts,
            sched_time, in_config_id, SYSTIMESTAMP
        ) RETURNING slot_id, scheduled_time
          INTO ou_slot_id, ou_scheduled_time;

        UPDATE rate_limit_window_counter
        SET slot_count = slot_count + 1
        WHERE window_start = window_ts;

        ou_window_start     := window_ts;
        ou_status           := STATUS_NEW;
        ou_windows_searched := windows_searched;
        RETURN TRUE;

    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            SELECT slot_id, scheduled_time, window_start
            INTO   ou_slot_id, ou_scheduled_time, ou_window_start
            FROM   rate_limit_event_slot
            WHERE  event_id = in_event_id;
            ou_status           := STATUS_EXISTING;
            ou_windows_searched := windows_searched;
            RETURN TRUE;
    END claim_slot;

---------------------------------------------------------------
-- MAIN BLOCK
---------------------------------------------------------------
BEGIN
    IF NOT check_existing_slot() THEN

        -- Phase 1: First window with proportional capacity
        ensure_window_exists(in_window_start);
        windows_searched := windows_searched + 1;

        current_count := try_lock_first_window(in_window_start);
        IF current_count >= 0 AND current_count < in_max_first_window THEN
            slot_claimed := claim_slot(in_window_start, in_first_jitter_ms);
        END IF;

        -- Phase 2: Frontier-tracked find+lock
        IF NOT slot_claimed THEN
            -- Step 1: Get (or initialize) provisioning frontier
            v_window_end := fetch_or_init_window_end();

            -- Step 2: find+lock over ENTIRE provisioned range
            found_window := find_and_lock(in_window_start + window_size, v_window_end);
            IF found_window IS NOT NULL THEN
                windows_searched := windows_searched + in_max_windows_in_chunk;
                slot_claimed := claim_slot(found_window, in_full_jitter_ms);
            END IF;

            -- Step 3: Extension loop — extend from frontier
            IF NOT slot_claimed THEN
                search_from := v_window_end;

                FOR chunk_idx IN 0 .. in_max_chunks_to_search - 1 LOOP
                    EXIT WHEN slot_claimed;

                    chunk_end := search_from + chunk_size;
                    ensure_chunk_provisioned(search_from);
                    windows_searched := windows_searched + in_max_windows_in_chunk;

                    -- Append new frontier row (catch DUP — no contention)
                    BEGIN
                        INSERT INTO track_window_end(requested_time, window_end)
                        VALUES (in_window_start, chunk_end);
                    EXCEPTION
                        WHEN DUP_VAL_ON_INDEX THEN NULL;
                    END;

                    found_window := find_and_lock(search_from, chunk_end);
                    IF found_window IS NOT NULL THEN
                        slot_claimed := claim_slot(found_window, in_full_jitter_ms);
                    END IF;

                    search_from := chunk_end;
                END LOOP;
            END IF;
        END IF;

        IF NOT slot_claimed THEN
            ou_windows_searched := windows_searched;
        END IF;
    END IF;

    -- Marshal ou_ locals -> OUT bind variables
    ? := ou_status;            /* 12 */
    ? := ou_slot_id;           /* 13 */
    ? := ou_scheduled_time;    /* 14 */
    ? := ou_window_start;      /* 15 */
    ? := ou_windows_searched;  /* 16 */
END;
