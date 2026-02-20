package com.ratelimiter.slot

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.config.RateLimitConfigRepository
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.sql.Timestamp
import java.sql.Types
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * Core rate limiting algorithm.
 *
 * Assigns scheduling slots to payment events by walking through time windows.
 * The entire slot assignment — idempotency check, window walk loop, lock acquisition,
 * slot insertion, and counter update — executes as a single anonymous PL/SQL block
 * in one JDBC round trip. This minimizes network latency and lock hold time.
 *
 * The PL/SQL block uses Oracle `SELECT FOR UPDATE NOWAIT` for pessimistic locking.
 * Threads that lose the lock race skip to the next window instead of blocking,
 * distributing load across windows naturally under concurrent access.
 *
 * The lookahead range is dynamic: it extends to `furthestAssignedWindow + headroom`
 * rather than using a fixed maximum. This self-adjusts to actual load patterns.
 */
@ApplicationScoped
class SlotAssignmentService @Inject constructor(
    private val configRepository: RateLimitConfigRepository,
    @param:ConfigProperty(name = "rate-limiter.headroom-windows", defaultValue = "100")
    private val headroomWindows: Int
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentService::class.java)

    companion object {
        /** Status codes returned by the PL/SQL block via the ou_status OUT parameter. */
        private const val STATUS_NEW = 1
        private const val STATUS_EXISTING = 0
        private const val STATUS_EXHAUSTED = -1

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
        private val ASSIGN_SLOT_PLSQL = """
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

    /**
     * High-water mark: the latest (furthest into the future) window that has ever
     * had a slot assigned across any thread on this node.
     *
     * This drives the **dynamic lookahead** — instead of a fixed MAX_LOOKAHEAD, the
     * search limit is computed as:
     *     searchLimit = max(furthestAssignedWindow, requestedWindowStart) + headroom
     *
     * Why this matters:
     * - Eliminates a static ceiling that must be tuned. The frontier advances
     *   organically with load — after 5,000 windows are filled, the search range
     *   is 5,000 + headroom, not a fixed 200.
     * - Prevents unbounded scans on sparse data: headroom caps how far past the
     *   frontier any single request will search (default 100 windows).
     * - Each node maintains its own AtomicReference. Inter-node skew is safe because
     *   the counter table is the source of truth for capacity; this field only bounds
     *   how far the node is willing to look. Nodes re-sync from DB on restart.
     *
     * Lifecycle:
     * - Initialized from DB at startup via [initFurthestWindow] (survives restarts).
     * - Advanced atomically on every successful slot assignment (never decremented).
     */
    private val furthestAssignedWindow = AtomicReference<Instant>(Instant.EPOCH)

    /**
     * Initializes [furthestAssignedWindow] from the database so the dynamic lookahead
     * survives application restarts.
     *
     * Without this, a freshly started node would begin with frontier = EPOCH, meaning
     * its search limit would be `requestedWindowStart + headroom` — potentially much
     * tighter than the actual frontier across the cluster. Events that should land in
     * windows far ahead (because earlier windows are full) could fail with
     * [SlotAssignmentException] until the node "catches up" by assigning enough slots.
     *
     * By reading `MAX(window_start) WHERE slot_count > 0`, we restore the frontier to
     * the latest window that any node has ever written to, giving this node the same
     * search range as nodes that were running continuously.
     */
    @PostConstruct
    fun initFurthestWindow() {
        try {
            val maxWindow = transaction {
                exec(
                    "SELECT MAX(window_start) AS max_ws FROM rate_limit_window_counter WHERE slot_count > 0",
                    emptyList()
                ) { rs ->
                    if (rs.next()) rs.getTimestamp("max_ws")?.toInstant() else null
                }
            }
            if (maxWindow != null) {
                furthestAssignedWindow.set(maxWindow)
                logger.info("Initialized furthestAssignedWindow from DB: {}", maxWindow)
            } else {
                logger.info("No existing window counters found, furthestAssignedWindow starts at EPOCH")
            }
        } catch (e: Exception) {
            logger.warn("Failed to initialize furthestAssignedWindow from DB, starting at EPOCH", e)
        }
    }

    /**
     * Assign a scheduling slot for the given event.
     *
     * Executes the entire operation — idempotency check, window walk, lock acquisition,
     * slot insertion, counter update — in a **single JDBC round trip** via an anonymous
     * PL/SQL block. The only Kotlin-side work after the call is interpreting the result
     * and advancing the frontier.
     *
     * @param eventId      unique event identifier (idempotency key)
     * @param configName   the rate limit config to use
     * @param requestedTime when the caller wants the event processed
     * @return assigned slot with event ID, scheduled time, and delay from requested time
     * @throws SlotAssignmentException if no window has capacity within the dynamic lookahead range
     * @throws ConfigLoadException if no active config exists for the given name
     */
    fun assignSlot(eventId: String, configName: String, requestedTime: Instant): AssignedSlot {
        // Step 1: Load config (from 5-second cache)
        val config = configRepository.loadActiveConfig(configName)
            ?: throw ConfigLoadException(
                configName,
                "No active rate limit config found for: $configName"
            )

        // Step 2: Compute starting window and dynamic search limit
        val windowStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
        val frontier = furthestAssignedWindow.get()
        val headroomDuration = Duration.ofSeconds(headroomWindows.toLong() * config.windowSizeSecs)
        val searchLimit = maxOf(frontier, windowStart).plus(headroomDuration)

        // Step 3: Single round trip — PL/SQL block does everything
        val result = executeSlotAssignment(eventId, windowStart, searchLimit, requestedTime, config)

        // Step 4: Interpret result (no DB, pure Kotlin)
        return when (result.status) {
            STATUS_NEW -> {
                // Advance the frontier atomically (monotonic max)
                furthestAssignedWindow.updateAndGet { current ->
                    if (result.windowStart.isAfter(current)) result.windowStart else current
                }
                logger.info(
                    "Assigned slot for eventId={} in window={} after searching {} windows",
                    eventId, result.windowStart, result.windowsSearched
                )
                buildAssignedSlot(eventId, result.scheduledTime, requestedTime)
            }

            STATUS_EXISTING -> {
                logger.debug("Idempotent hit for eventId={}", eventId)
                buildAssignedSlot(eventId, result.scheduledTime, requestedTime)
            }

            STATUS_EXHAUSTED -> {
                throw SlotAssignmentException(
                    eventId = eventId,
                    windowsSearched = result.windowsSearched,
                    message = "Could not assign slot for event $eventId after searching " +
                            "${result.windowsSearched} windows (frontier=$frontier, searchLimit=$searchLimit)"
                )
            }

            else -> error("Unexpected PL/SQL status: ${result.status}")
        }
    }

    /**
     * Marshalling result from the PL/SQL block's OUT parameters.
     */
    private data class PlSqlResult(
        val status: Int,
        val slotId: Long,
        val scheduledTime: Instant,
        val windowStart: Instant,
        val windowsSearched: Int
    )

    /**
     * Execute the PL/SQL slot assignment block via CallableStatement.
     * Single JDBC round trip: binds 8 IN params, registers 5 OUT params, executes,
     * reads results.
     */
    private fun executeSlotAssignment(
        eventId: String,
        windowStart: Instant,
        searchLimit: Instant,
        requestedTime: Instant,
        config: RateLimitConfig
    ): PlSqlResult {
        return transaction {
            val rawConnection = this.connection.connection as java.sql.Connection
            rawConnection.prepareCall(ASSIGN_SLOT_PLSQL).use { cs ->
                // Bind IN parameters (positions 1-8)
                cs.setString(1, eventId)                              // in_event_id
                cs.setTimestamp(2, Timestamp.from(windowStart))       // in_window_start
                cs.setTimestamp(3, Timestamp.from(searchLimit))       // in_search_limit
                cs.setTimestamp(4, Timestamp.from(requestedTime))     // in_requested_time
                cs.setLong(5, config.configId)                        // in_config_id
                cs.setInt(6, config.maxPerWindow)                     // in_max_per_window
                cs.setLong(7, config.windowSizeSecs)                  // in_window_size_secs
                cs.setLong(8, config.windowSizeMs)                    // in_window_size_ms

                // Register OUT parameters (positions 9-13)
                cs.registerOutParameter(9, Types.INTEGER)             // ou_status
                cs.registerOutParameter(10, Types.BIGINT)             // ou_slot_id
                cs.registerOutParameter(11, Types.TIMESTAMP)          // ou_scheduled_time
                cs.registerOutParameter(12, Types.TIMESTAMP)          // ou_window_start
                cs.registerOutParameter(13, Types.INTEGER)            // ou_windows_searched

                cs.execute()

                PlSqlResult(
                    status = cs.getInt(9),
                    slotId = cs.getLong(10),
                    scheduledTime = cs.getTimestamp(11)?.toInstant() ?: Instant.EPOCH,
                    windowStart = cs.getTimestamp(12)?.toInstant() ?: Instant.EPOCH,
                    windowsSearched = cs.getInt(13)
                )
            }
        }
    }

    /**
     * Build the public [AssignedSlot] from PL/SQL results, computing the delay.
     */
    private fun buildAssignedSlot(
        eventId: String,
        scheduledTime: Instant,
        requestedTime: Instant
    ): AssignedSlot {
        val delay = Duration.between(requestedTime, scheduledTime).let { d ->
            if (d.isNegative) Duration.ZERO else d
        }
        return AssignedSlot(eventId = eventId, scheduledTime = scheduledTime, delay = delay)
    }

    /**
     * Align a timestamp to the nearest window boundary at or before the given time.
     * Windows are aligned to epoch in multiples of windowSizeSecs.
     */
    private fun alignToWindowBoundary(time: Instant, windowSizeSecs: Long): Instant {
        val epochSecond = time.epochSecond
        val alignedEpoch = epochSecond - (epochSecond % windowSizeSecs)
        return Instant.ofEpochSecond(alignedEpoch)
    }
}
