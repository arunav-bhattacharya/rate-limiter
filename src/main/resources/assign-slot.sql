DECLARE
    -- IN bind variables
    in_event_id           VARCHAR2(256) := ?;  /* 1  */
    in_window_start       TIMESTAMP     := ?;  /* 2  */
    in_requested_time     TIMESTAMP     := ?;  /* 3  */
    in_config_id          VARCHAR2(50)  := ?;  /* 4  */
    in_max_per_window     NUMBER        := ?;  /* 5  */
    in_window_size_secs   NUMBER        := ?;  /* 6  */
    in_max_first_window   NUMBER        := ?;  /* 7  */
    in_first_jitter_ms    NUMBER        := ?;  /* 8  */
    in_full_jitter_ms     NUMBER        := ?;  /* 9  */
    in_headroom_secs      NUMBER        := ?;  /* 10 */
    in_max_search_chunks  NUMBER        := ?;  /* 11 */

    -- Output result locals (marshalled to OUT bind vars at end)
    ou_status           NUMBER := -1;  -- default: EXHAUSTED
    ou_slot_id          VARCHAR2(50);
    ou_scheduled_time   TIMESTAMP;
    ou_window_start     TIMESTAMP;
    ou_windows_searched NUMBER := 0;

    -- Working state
    window_size      INTERVAL DAY TO SECOND := NUMTODSINTERVAL(in_window_size_secs, 'SECOND');
    headroom         INTERVAL DAY TO SECOND := NUMTODSINTERVAL(in_headroom_secs, 'SECOND');
    current_window   TIMESTAMP;
    current_count    NUMBER;
    windows_searched NUMBER := 0;
    slot_claimed     BOOLEAN := FALSE;
    first_available  TIMESTAMP;
    search_limit     TIMESTAMP;
    chunk_num        NUMBER := 0;

    STATUS_NEW       CONSTANT NUMBER := 1;
    STATUS_EXISTING  CONSTANT NUMBER := 0;
    STATUS_EXHAUSTED CONSTANT NUMBER := -1;

    ---------------------------------------------------------------
    -- check_existing_slot: Idempotency pre-check.
    -- Returns TRUE if event already has a slot (populates ou_ locals).
    ---------------------------------------------------------------

    FUNCTION check_existing_slot RETURN BOOLEAN IS
    BEGIN
        SELECT  WNDW_SLOT_ID, COMPUTED_SCHED_TS, WNDW_STRT_TS
        INTO    ou_slot_id, ou_scheduled_time, ou_window_start
        FROM    RL_EVENT_SLOT_DTL
        WHERE   EVENT_ID = in_event_id;
        ou_status           := STATUS_EXISTING;
        ou_windows_searched := 0;
        RETURN TRUE;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN FALSE;
    END check_existing_slot;

    ---------------------------------------------------------------
    -- try_lock_window: Ensure counter row exists, then lock + read.
    -- Uses SKIP LOCKED: if the row is locked by another session,
    -- the SELECT silently returns no rows instead of raising an
    -- exception. Returns SLOT_CT on success, -1 if skipped.
    ---------------------------------------------------------------

    FUNCTION try_lock_window(window_ts IN TIMESTAMP) RETURN NUMBER IS
        locked_count NUMBER;
    BEGIN
        BEGIN
            INSERT INTO RL_WNDW_CT(WNDW_STRT_TS, SLOT_CT, CREAT_TS)
            VALUES (window_ts, 0, SYSTIMESTAMP);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
        END;

        SELECT SLOT_CT INTO locked_count
        FROM   RL_WNDW_CT
        WHERE  WNDW_STRT_TS = window_ts
        FOR UPDATE SKIP LOCKED;

        RETURN locked_count;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- SKIP LOCKED: row exists but is locked by another session
            RETURN -1;
    END try_lock_window;

    ---------------------------------------------------------------
    -- claim_slot_in_window: Insert slot + increment counter.
    -- Jitter is pre-computed in Kotlin and passed as p_jitter_ms.
    ---------------------------------------------------------------

    FUNCTION claim_slot_in_window(
        window_ts    IN TIMESTAMP,
        slot_count   IN NUMBER,
        p_jitter_ms  IN NUMBER
    ) RETURN BOOLEAN IS
        jitter     INTERVAL DAY TO SECOND := NUMTODSINTERVAL(p_jitter_ms / 1000, 'SECOND');
        sched_time TIMESTAMP;
        v_slot_id  VARCHAR2(50);
    BEGIN
        sched_time := window_ts + jitter;
        v_slot_id  := SYS_GUID();

        INSERT INTO RL_EVENT_SLOT_DTL(
            WNDW_SLOT_ID, EVENT_ID, REQ_TS, WNDW_STRT_TS,
            COMPUTED_SCHED_TS, RL_WNDW_CONFIG_ID, CREAT_TS
        ) VALUES (
            v_slot_id, in_event_id, in_requested_time, window_ts,
            sched_time, in_config_id, SYSTIMESTAMP
        );

        ou_slot_id          := v_slot_id;
        ou_scheduled_time   := sched_time;

        UPDATE RL_WNDW_CT
        SET SLOT_CT = SLOT_CT + 1
        WHERE WNDW_STRT_TS = window_ts;

        ou_window_start     := window_ts;
        ou_status           := STATUS_NEW;
        ou_windows_searched := windows_searched;
        RETURN TRUE;

    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            SELECT WNDW_SLOT_ID, COMPUTED_SCHED_TS, WNDW_STRT_TS
            INTO   ou_slot_id, ou_scheduled_time, ou_window_start
            FROM   RL_EVENT_SLOT_DTL
            WHERE  EVENT_ID = in_event_id;
            ou_status           := STATUS_EXISTING;
            ou_windows_searched := windows_searched;
            RETURN TRUE;
    END claim_slot_in_window;

    ---------------------------------------------------------------
    -- find_first_available_window: Adjacent-first + index fallback.
    -- 1. Check next sequential window (most common case, O(1) PK lookup)
    -- 2. If full, index scan for first non-full window
    -- 3. If all existing full, jump past last counter row
    ---------------------------------------------------------------

    FUNCTION find_first_available_window(start_ts IN TIMESTAMP) RETURN TIMESTAMP IS
        next_ts   TIMESTAMP := start_ts + window_size;
        cnt       NUMBER;
        open_ts   TIMESTAMP;
        max_ts    TIMESTAMP;
    BEGIN
        -- 1. Check the immediately next window (most common case)
        BEGIN
            SELECT SLOT_CT INTO cnt
            FROM   RL_WNDW_CT
            WHERE  WNDW_STRT_TS = next_ts;

            -- Row exists: available if not full
            IF cnt < in_max_per_window THEN
                RETURN next_ts;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- No counter row = empty window = available
                RETURN next_ts;
        END;

        -- 2. Next window is full. Use index to find first non-full window
        SELECT MIN(WNDW_STRT_TS) INTO open_ts
        FROM   RL_WNDW_CT
        WHERE  SLOT_CT < in_max_per_window
          AND  WNDW_STRT_TS > start_ts;

        IF open_ts IS NOT NULL THEN
            RETURN open_ts;
        END IF;

        -- 3. All existing windows are full. Jump past the last counter row.
        SELECT MAX(WNDW_STRT_TS) INTO max_ts
        FROM   RL_WNDW_CT
        WHERE  WNDW_STRT_TS > start_ts;

        IF max_ts IS NOT NULL THEN
            RETURN max_ts + window_size;
        ELSE
            RETURN next_ts;
        END IF;
    END find_first_available_window;

---------------------------------------------------------------
-- PROCEDURE BODY: Main slot assignment logic
---------------------------------------------------------------
BEGIN
    -- 1. Idempotency pre-check
    IF NOT check_existing_slot() THEN

        -- Phase 1: Try first window with pre-computed proportional capacity
        current_window := in_window_start;
        windows_searched := windows_searched + 1;

        current_count := try_lock_window(current_window);
        IF current_count >= 0 AND current_count < in_max_first_window THEN
            slot_claimed := claim_slot_in_window(current_window, current_count, in_first_jitter_ms);
        END IF;

        -- Phase 2: Chunked adaptive search
        WHILE NOT slot_claimed AND chunk_num < in_max_search_chunks LOOP
            chunk_num := chunk_num + 1;

            IF chunk_num = 1 THEN
                first_available := find_first_available_window(in_window_start);
            ELSE
                first_available := find_first_available_window(current_window);
            END IF;

            search_limit := first_available + headroom;
            current_window := first_available;

            WHILE current_window <= search_limit AND NOT slot_claimed LOOP
                windows_searched := windows_searched + 1;

                current_count := try_lock_window(current_window);
                IF current_count >= 0 AND current_count < in_max_per_window THEN
                    slot_claimed := claim_slot_in_window(current_window, current_count, in_full_jitter_ms);
                END IF;

                IF NOT slot_claimed THEN
                    current_window := current_window + window_size;
                END IF;
            END LOOP;
        END LOOP;

        -- If never claimed, status stays EXHAUSTED
        IF NOT slot_claimed THEN
            ou_windows_searched := windows_searched;
        END IF;
    END IF;

    -- Marshal ou_ locals -> OUT bind variables (always reached)
    ? := ou_status;            /* 12 */
    ? := ou_slot_id;           /* 13 */
    ? := ou_scheduled_time;    /* 14 */
    ? := ou_window_start;      /* 15 */
    ? := ou_windows_searched;  /* 16 */
END;
