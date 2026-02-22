package com.ratelimiter.slot

/**
 * SQL constants for the slot assignment algorithm.
 */
internal object SlotAssignmentSql {

    /** Status codes returned by the PL/SQL block via the ou_status OUT parameter. */
    const val STATUS_NEW = 1
    const val STATUS_EXISTING = 0
    const val STATUS_EXHAUSTED = -1

    const val REASON_CAPACITY_EXHAUSTED = "CAPACITY_EXHAUSTED"
    const val REASON_LOCK_CONTENTION = "LOCK_CONTENTION"
    const val REASON_OVERLOADED = "OVERLOADED"

    /**
     * Parameter positions:
     *   IN:  1=event_id, 2=window_start, 3=requested_time, 4=config_id,
     *        5=max_per_window, 6=window_size_secs, 7=max_first_window,
     *        8=first_jitter_ms, 9=full_jitter_ms, 10=headroom_secs,
     *        11=horizon_buffer_secs
     *   OUT: 12=status, 13=slot_id, 14=scheduled_time, 15=window_start_out,
     *        16=windows_searched, 17=resume_window_start, 18=search_limit,
     *        19=exhaust_reason
     */
    val ASSIGN_SLOT_PLSQL = """
        DECLARE
            -- IN bind variables
            in_event_id           VARCHAR2(256) := ?;  /* 1  */
            in_window_start       TIMESTAMP     := ?;  /* 2  */
            in_requested_time     TIMESTAMP     := ?;  /* 3  */
            in_config_id          NUMBER        := ?;  /* 4  */
            in_max_per_window     NUMBER        := ?;  /* 5  */
            in_window_size_secs   NUMBER        := ?;  /* 6  */
            in_max_first_window   NUMBER        := ?;  /* 7  */
            in_first_jitter_ms    NUMBER        := ?;  /* 8  */
            in_full_jitter_ms     NUMBER        := ?;  /* 9  */
            in_headroom_secs      NUMBER        := ?;  /* 10 */
            in_horizon_buffer_secs NUMBER       := ?;  /* 11 */

            -- Output result locals (marshalled to OUT bind vars at end)
            ou_status             NUMBER := -1;  -- default: EXHAUSTED
            ou_slot_id            NUMBER;
            ou_scheduled_time     TIMESTAMP;
            ou_window_start       TIMESTAMP;
            ou_windows_searched   NUMBER := 0;
            ou_resume_window_start TIMESTAMP;
            ou_search_limit       TIMESTAMP;
            ou_exhaust_reason     VARCHAR2(64);

            -- Working state
            window_size      INTERVAL DAY TO SECOND := NUMTODSINTERVAL(in_window_size_secs, 'SECOND');
            headroom         INTERVAL DAY TO SECOND := NUMTODSINTERVAL(in_headroom_secs, 'SECOND');
            horizon_buffer   INTERVAL DAY TO SECOND := NUMTODSINTERVAL(in_horizon_buffer_secs, 'SECOND');
            windows_searched NUMBER := 0;
            slot_claimed     BOOLEAN := FALSE;
            search_limit     TIMESTAMP;
            required_horizon TIMESTAMP;
            horizon_ready    NUMBER := 1;

            STATUS_NEW       CONSTANT NUMBER := 1;
            STATUS_EXISTING  CONSTANT NUMBER := 0;
            STATUS_EXHAUSTED CONSTANT NUMBER := -1;
            REASON_CAPACITY  CONSTANT VARCHAR2(64) := 'CAPACITY_EXHAUSTED';
            REASON_LOCK      CONSTANT VARCHAR2(64) := 'LOCK_CONTENTION';

            ---------------------------------------------------------------
            -- check_existing_slot: idempotency pre-check.
            ---------------------------------------------------------------
            FUNCTION check_existing_slot RETURN BOOLEAN IS
            BEGIN
                SELECT slot_id, scheduled_time, window_start
                INTO   ou_slot_id, ou_scheduled_time, ou_window_start
                FROM   rate_limit_event_slot
                WHERE  event_id = in_event_id;

                ou_status             := STATUS_EXISTING;
                ou_windows_searched   := 0;
                ou_resume_window_start := NULL;
                ou_search_limit       := NULL;
                ou_exhaust_reason     := NULL;
                RETURN TRUE;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RETURN FALSE;
            END check_existing_slot;

            ---------------------------------------------------------------
            -- ensure_horizon: materialize rows for this request range.
            -- Returns 1 if rows are ready, 0 when another tx is extending.
            ---------------------------------------------------------------
            FUNCTION ensure_horizon(required_horizon_ts IN TIMESTAMP) RETURN NUMBER IS
                current_horizon_start TIMESTAMP;
                current_horizon_end   TIMESTAMP;
                state_window_size     NUMBER;
            BEGIN
                BEGIN
                    SELECT horizon_start, horizon_end, window_size_secs
                    INTO   current_horizon_start, current_horizon_end, state_window_size
                    FROM   rate_limit_horizon_state
                    WHERE  horizon_key = 'GLOBAL'
                    FOR UPDATE WAIT 1;
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE IN (-54, -60, -30006) THEN
                            SELECT horizon_start, horizon_end
                            INTO   current_horizon_start, current_horizon_end
                            FROM   rate_limit_horizon_state
                            WHERE  horizon_key = 'GLOBAL';

                            IF current_horizon_start <= in_window_start
                               AND current_horizon_end >= required_horizon_ts THEN
                                RETURN 1;
                            END IF;
                            RETURN 0;
                        END IF;
                        RAISE;
                END;

                IF state_window_size IS NOT NULL AND state_window_size <> in_window_size_secs THEN
                    RAISE_APPLICATION_ERROR(-20002, 'Global window_size mismatch in horizon state');
                END IF;

                IF current_horizon_start <= in_window_start
                   AND current_horizon_end >= required_horizon_ts THEN
                    RETURN 1;
                END IF;

                MERGE INTO rate_limit_window_counter c
                USING (
                    SELECT in_window_start + NUMTODSINTERVAL((LEVEL - 1) * in_window_size_secs, 'SECOND') AS window_start
                    FROM   dual
                    CONNECT BY in_window_start + NUMTODSINTERVAL((LEVEL - 1) * in_window_size_secs, 'SECOND')
                               <= required_horizon_ts
                ) s
                ON (c.window_start = s.window_start)
                WHEN NOT MATCHED THEN
                    INSERT (window_start, slot_count, status)
                    VALUES (s.window_start, 0, 'OPEN');

                UPDATE rate_limit_horizon_state
                SET    horizon_start = in_window_start,
                       horizon_end = required_horizon_ts,
                       window_size_secs = in_window_size_secs,
                       updated_at = SYSTIMESTAMP
                WHERE  horizon_key = 'GLOBAL';

                RETURN 1;
            END ensure_horizon;

            ---------------------------------------------------------------
            -- claim_slot_in_window: insert slot + increment counter.
            ---------------------------------------------------------------
            FUNCTION claim_slot_in_window(
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
            -- try_claim_specific_window: lock one exact window with SKIP LOCKED.
            ---------------------------------------------------------------
            FUNCTION try_claim_specific_window(
                window_ts   IN TIMESTAMP,
                max_allowed IN NUMBER,
                p_jitter_ms IN NUMBER
            ) RETURN BOOLEAN IS
                current_count NUMBER;
            BEGIN
                windows_searched := windows_searched + 1;
                SELECT slot_count INTO current_count
                FROM   rate_limit_window_counter
                WHERE  window_start = window_ts
                  AND  slot_count < max_allowed
                FOR UPDATE SKIP LOCKED;

                RETURN claim_slot_in_window(window_ts, p_jitter_ms);
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RETURN FALSE;
            END try_claim_specific_window;

            ---------------------------------------------------------------
            -- try_claim_first_available: earliest available row in bounded range.
            ---------------------------------------------------------------
            FUNCTION try_claim_first_available(
                start_ts    IN TIMESTAMP,
                end_ts      IN TIMESTAMP,
                p_jitter_ms IN NUMBER
            ) RETURN BOOLEAN IS
                candidate_window TIMESTAMP;
                CURSOR c_available(p_start_ts TIMESTAMP, p_end_ts TIMESTAMP) IS
                    SELECT window_start
                    FROM   rate_limit_window_counter
                    WHERE  window_start >= p_start_ts
                      AND  window_start <= p_end_ts
                      AND  slot_count < in_max_per_window
                    ORDER BY window_start
                    FOR UPDATE SKIP LOCKED;
            BEGIN
                windows_searched := windows_searched + 1;
                OPEN c_available(start_ts, end_ts);
                FETCH c_available INTO candidate_window;
                IF c_available%NOTFOUND THEN
                    CLOSE c_available;
                    RETURN FALSE;
                END IF;
                CLOSE c_available;

                RETURN claim_slot_in_window(candidate_window, p_jitter_ms);
            EXCEPTION
                WHEN OTHERS THEN
                    IF c_available%ISOPEN THEN
                        CLOSE c_available;
                    END IF;
                    RAISE;
            END try_claim_first_available;

            ---------------------------------------------------------------
            -- classify_exhaust_reason: capacity vs lock contention.
            ---------------------------------------------------------------
            PROCEDURE classify_exhaust_reason(
                start_ts   IN TIMESTAMP,
                end_ts     IN TIMESTAMP,
                horizon_ok IN NUMBER
            ) IS
                eligible_count NUMBER := 0;
            BEGIN
                IF horizon_ok = 0 THEN
                    ou_exhaust_reason := REASON_LOCK;
                    ou_resume_window_start := start_ts;
                    RETURN;
                END IF;

                SELECT COUNT(*)
                INTO   eligible_count
                FROM   rate_limit_window_counter
                WHERE  window_start >= start_ts
                  AND  window_start <= end_ts
                  AND  slot_count < in_max_per_window;

                IF eligible_count > 0 THEN
                    ou_exhaust_reason := REASON_LOCK;
                    ou_resume_window_start := start_ts;
                ELSE
                    ou_exhaust_reason := REASON_CAPACITY;
                    ou_resume_window_start := end_ts + window_size;
                END IF;
            END classify_exhaust_reason;

        BEGIN
            search_limit := in_window_start + headroom;
            required_horizon := search_limit + horizon_buffer;
            ou_search_limit := search_limit;
            ou_resume_window_start := in_window_start;

            IF NOT check_existing_slot() THEN
                horizon_ready := ensure_horizon(required_horizon);

                IF horizon_ready = 1 THEN
                    slot_claimed := try_claim_specific_window(
                        in_window_start, in_max_first_window, in_first_jitter_ms
                    );
                END IF;

                IF NOT slot_claimed AND horizon_ready = 1 THEN
                    slot_claimed := try_claim_first_available(
                        in_window_start + window_size, search_limit, in_full_jitter_ms
                    );
                END IF;

                IF NOT slot_claimed THEN
                    ou_windows_searched := windows_searched;
                    classify_exhaust_reason(in_window_start, search_limit, horizon_ready);
                END IF;
            END IF;

            ? := ou_status;               /* 12 */
            ? := ou_slot_id;              /* 13 */
            ? := ou_scheduled_time;       /* 14 */
            ? := ou_window_start;         /* 15 */
            ? := ou_windows_searched;     /* 16 */
            ? := ou_resume_window_start;  /* 17 */
            ? := ou_search_limit;         /* 18 */
            ? := ou_exhaust_reason;       /* 19 */
        END;
    """.trimIndent()
}
