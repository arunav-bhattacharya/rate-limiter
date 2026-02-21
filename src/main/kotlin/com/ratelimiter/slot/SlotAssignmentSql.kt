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
     * - try_lock_window: Ensure counter row + SELECT FOR UPDATE NOWAIT
     * - claim_slot_in_window: INSERT slot + UPDATE counter with status transition, with ORA-00001 handling
     * - find_first_open_window: Skip query using status index to find first OPEN window
     *
     * The main block uses a two-phase approach:
     *   Phase 1: Try the first (floor-aligned) window with pre-computed proportional capacity
     *   Phase 2: Skip to first OPEN window via index scan, compute dynamic search limit, walk
     *
     * Jitter and proportional capacity are pre-computed in Kotlin and passed as IN params.
     *
     * Parameter positions:
     *   IN:  1=event_id, 2=window_start, 3=requested_time, 4=config_id,
     *        5=max_per_window, 6=window_size_secs, 7=max_first_window,
     *        8=first_jitter_ms, 9=full_jitter_ms, 10=headroom_secs
     *   OUT: 11=status, 12=slot_id, 13=scheduled_time, 14=window_start_out,
     *        15=windows_searched
     */
    val ASSIGN_SLOT_PLSQL = """
        DECLARE
            -- IN bind variables
            in_event_id         VARCHAR2(256) := ?;  /* 1  */
            in_window_start     TIMESTAMP     := ?;  /* 2  */
            in_requested_time   TIMESTAMP     := ?;  /* 3  */
            in_config_id        NUMBER        := ?;  /* 4  */
            in_max_per_window   NUMBER        := ?;  /* 5  */
            in_window_size_secs NUMBER        := ?;  /* 6  */
            in_max_first_window NUMBER        := ?;  /* 7  */
            in_first_jitter_ms  NUMBER        := ?;  /* 8  */
            in_full_jitter_ms   NUMBER        := ?;  /* 9  */
            in_headroom_secs    NUMBER        := ?;  /* 10 */

            -- Output result locals (marshalled to OUT bind vars at end)
            ou_status           NUMBER := -1;  -- default: EXHAUSTED
            ou_slot_id          NUMBER;
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
            first_open_window TIMESTAMP;
            search_limit     TIMESTAMP;

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
            -- try_lock_window: Ensure counter row exists, then lock + read.
            -- Returns slot_count on success, -1 if contended/deadlocked.
            -- Creates new rows with status = 'OPEN'.
            ---------------------------------------------------------------

            FUNCTION try_lock_window(window_ts IN TIMESTAMP) RETURN NUMBER IS
                locked_count NUMBER;
            BEGIN
                BEGIN
                    INSERT INTO rate_limit_window_counter(window_start, slot_count, status)
                    VALUES (window_ts, 0, 'OPEN');
                EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
                END;

                SELECT slot_count INTO locked_count
                FROM   rate_limit_window_counter
                WHERE  window_start = window_ts
                FOR UPDATE NOWAIT;

                RETURN locked_count;
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE IN (-54, -60) THEN
                        RETURN -1;
                    END IF;
                    RAISE;
            END try_lock_window;

            ---------------------------------------------------------------
            -- claim_slot_in_window: Insert slot + increment counter.
            -- Atomically sets status to CLOSED when slot_count reaches max.
            -- Jitter is pre-computed in Kotlin and passed as p_jitter_ms.
            ---------------------------------------------------------------

            FUNCTION claim_slot_in_window(
                window_ts    IN TIMESTAMP,
                slot_count   IN NUMBER,
                p_jitter_ms  IN NUMBER
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

                -- Atomic: increment counter AND set CLOSED if full
                UPDATE rate_limit_window_counter
                SET slot_count = slot_count + 1,
                    status = CASE WHEN slot_count + 1 >= in_max_per_window THEN 'CLOSED' ELSE 'OPEN' END
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
            END claim_slot_in_window;

            ---------------------------------------------------------------
            -- find_first_open_window: Skip query using status index.
            -- Returns the first OPEN window after start_ts, or the first
            -- empty (no counter row) window past the last counter row.
            ---------------------------------------------------------------

            FUNCTION find_first_open_window(start_ts IN TIMESTAMP) RETURN TIMESTAMP IS
                open_ts TIMESTAMP;
                max_ts  TIMESTAMP;
            BEGIN
                -- Single indexed query: find first OPEN window after start_ts
                SELECT MIN(window_start) INTO open_ts
                FROM   rate_limit_window_counter
                WHERE  status = 'OPEN'
                  AND  window_start > start_ts;

                IF open_ts IS NOT NULL THEN
                    RETURN open_ts;
                END IF;

                -- No OPEN rows found. Jump past the last counter row.
                SELECT MAX(window_start) INTO max_ts
                FROM   rate_limit_window_counter
                WHERE  window_start > start_ts;

                IF max_ts IS NOT NULL THEN
                    RETURN max_ts + window_size;
                ELSE
                    -- Returning the next window after start_ts, as the first window was already checked in Phase 1
                    RETURN start_ts + window_size;
                END IF;
            END find_first_open_window;

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

                -- Phase 2: Skip to first OPEN window, compute search limit, walk
                IF NOT slot_claimed THEN
                    first_open_window  := find_first_open_window(in_window_start);
                    search_limit := first_open_window + headroom;
                    current_window := first_open_window;

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
                END IF;

                -- 3. If never claimed, status stays EXHAUSTED
                IF NOT slot_claimed THEN
                    ou_windows_searched := windows_searched;
                END IF;
            END IF;

            -- Marshal ou_ locals -> OUT bind variables (always reached)
            ? := ou_status;            /* 11 */
            ? := ou_slot_id;           /* 12 */
            ? := ou_scheduled_time;    /* 13 */
            ? := ou_window_start;      /* 14 */
            ? := ou_windows_searched;  /* 15 */
        END;
    """.trimIndent()
}
