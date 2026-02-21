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
     * database round trip. Contains three local functions:
     *
     * - check_existing_slot: Idempotency pre-check (SELECT by event_id)
     * - try_lock_window: Ensure counter row + SELECT FOR UPDATE NOWAIT
     * - claim_slot_in_window: INSERT slot + UPDATE counter, with ORA-00001 handling
     *
     * Parameter positions:
     *   IN:  1=event_id, 2=window_start, 3=search_limit, 4=requested_time,
     *        5=config_id, 6=max_per_window, 7=window_size_secs, 8=window_size_ms
     *   OUT: 9=status, 10=slot_id, 11=scheduled_time, 12=window_start_out,
     *        13=windows_searched
     */
    val ASSIGN_SLOT_PLSQL = """
        DECLARE
            -- IN bind variables
            in_event_id         VARCHAR2(256) := ?;  /* 1  */
            in_window_start     TIMESTAMP     := ?;  /* 2  */
            in_search_limit     TIMESTAMP     := ?;  /* 3  */
            in_requested_time   TIMESTAMP     := ?;  /* 4  */
            in_config_id        NUMBER        := ?;  /* 5  */
            in_max_per_window   NUMBER        := ?;  /* 6  */
            in_window_size_secs NUMBER        := ?;  /* 7  */
            in_window_size_ms   NUMBER        := ?;  /* 8  */

            -- Output result locals (marshalled to OUT bind vars at end)
            ou_status           NUMBER := -1;  -- default: EXHAUSTED
            ou_slot_id          NUMBER;
            ou_scheduled_time   TIMESTAMP;
            ou_window_start     TIMESTAMP;
            ou_windows_searched NUMBER := 0;

            -- Working state
            current_window   TIMESTAMP;
            current_count    NUMBER;
            jitter_ms        NUMBER;
            sched_time       TIMESTAMP;
            windows_searched NUMBER := 0;
            slot_claimed     BOOLEAN := FALSE;

            STATUS_NEW       CONSTANT NUMBER := 1;
            STATUS_EXISTING  CONSTANT NUMBER := 0;
            STATUS_EXHAUSTED CONSTANT NUMBER := -1;

            ---------------------------------------------------------------
            -- check_existing_slot: Idempotency pre-check.
            -- Returns TRUE if event already has a slot (populates ou_ locals).
            ---------------------------------------------------------------
            FUNCTION check_existing_slot RETURN BOOLEAN IS
            BEGIN
                SELECT slot_id, scheduled_time, window_start
                INTO ou_slot_id, ou_scheduled_time, ou_window_start
                FROM rate_limit_event_slot
                WHERE event_id = in_event_id;
                ou_status           := STATUS_EXISTING;
                ou_windows_searched := 0;
                RETURN TRUE;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN RETURN FALSE;
            END check_existing_slot;

            ---------------------------------------------------------------
            -- try_lock_window: Ensure counter row exists, then lock + read.
            -- Returns slot_count on success, -1 if contended/deadlocked.
            ---------------------------------------------------------------
            FUNCTION try_lock_window(window_ts IN TIMESTAMP) RETURN NUMBER IS
                locked_count NUMBER;
            BEGIN
                BEGIN
                    INSERT INTO rate_limit_window_counter(window_start, slot_count)
                    VALUES (window_ts, 0);
                EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
                END;

                SELECT slot_count INTO locked_count
                FROM rate_limit_window_counter
                WHERE window_start = window_ts
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
            -- Handles idempotency race (ORA-00001). Returns TRUE on success.
            ---------------------------------------------------------------
            FUNCTION claim_slot_in_window(
                window_ts  IN TIMESTAMP,
                slot_count IN NUMBER
            ) RETURN BOOLEAN IS
            BEGIN
                jitter_ms  := TRUNC(DBMS_RANDOM.VALUE(0, in_window_size_ms));
                sched_time := window_ts + NUMTODSINTERVAL(jitter_ms / 1000, 'SECOND');

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
                    INTO ou_slot_id, ou_scheduled_time, ou_window_start
                    FROM rate_limit_event_slot
                    WHERE event_id = in_event_id;
                    ou_status           := STATUS_EXISTING;
                    ou_windows_searched := windows_searched;
                    RETURN TRUE;
            END claim_slot_in_window;

        ---------------------------------------------------------------
        BEGIN
            -- 1. Idempotency pre-check
            IF NOT check_existing_slot() THEN

                -- 2. Window walk loop
                current_window := in_window_start;
                WHILE current_window <= in_search_limit AND NOT slot_claimed LOOP
                    windows_searched := windows_searched + 1;

                    current_count := try_lock_window(current_window);

                    IF current_count >= 0 AND current_count < in_max_per_window THEN
                        slot_claimed := claim_slot_in_window(current_window, current_count);
                    END IF;

                    IF NOT slot_claimed THEN
                        current_window := current_window
                            + NUMTODSINTERVAL(in_window_size_secs, 'SECOND');
                    END IF;
                END LOOP;

                -- 3. If never claimed, status stays EXHAUSTED
                IF NOT slot_claimed THEN
                    ou_windows_searched := windows_searched;
                END IF;
            END IF;

            -- Marshal ou_ locals -> OUT bind variables (always reached)
            ? := ou_status;            /* 9  */
            ? := ou_slot_id;           /* 10 */
            ? := ou_scheduled_time;    /* 11 */
            ? := ou_window_start;      /* 12 */
            ? := ou_windows_searched;  /* 13 */
        END;
    """.trimIndent()
}
