-- ============================================================================
-- TABLE: rate_limit_config
-- ============================================================================
-- Stores rate limit configuration parameters. Supports dynamic updates via
-- versioned rows: inserting a new config deactivates the previous one.
-- The application caches the active config in memory (5s TTL) and loads
-- from this table on cache miss.
--
-- Multiple config names can coexist (e.g., "default", "high-priority") but
-- only one row per config_name should have is_active = 1 at any time.
-- Old rows are kept for audit; never delete — deactivate instead.
-- ============================================================================
-- Sequence for rate_limit_config primary key.
-- Named to match Exposed ORM's autoIncrement() convention: {table}_{column}_seq
CREATE SEQUENCE rate_limit_config_config_id_seq START WITH 1 INCREMENT BY 1 NOCACHE;

CREATE TABLE rate_limit_config (
    -- Surrogate primary key, auto-generated via sequence.
    config_id        NUMBER(19) DEFAULT rate_limit_config_config_id_seq.NEXTVAL PRIMARY KEY,

    -- Logical name identifying this config (e.g., "default", "bulk-payments").
    -- Multiple rows can share the same name (versioning), but only one should
    -- be active at a time.
    config_name      VARCHAR2(128)  NOT NULL,

    -- Maximum number of events allowed per time window. This is the capacity
    -- check used by SlotAssignmentService: if slot_count >= max_per_window,
    -- the window is considered full.
    max_per_window   NUMBER(10)     NOT NULL,

    -- Duration of each time window as an ISO-8601 duration string
    -- (e.g., 'PT4S' = 4 seconds, 'PT1M30S' = 90 seconds, 'PT500MS' = 500ms).
    -- Stored as a string for flexibility; parsed to java.time.Duration at load time.
    -- Changing this while events are in-flight breaks window alignment —
    -- existing counter rows become meaningless. Only change between quiet periods.
    window_size      VARCHAR2(50)   NOT NULL,

    -- Timestamp from which this config version takes effect. Used for auditing
    -- when a config change was intended to go live.
    effective_from   TIMESTAMP      NOT NULL,

    -- Whether this config version is the current active one.
    -- 1 = active (used by the application), 0 = superseded/deactivated.
    -- Only one row per config_name should be active at any time.
    is_active        NUMBER(1)      DEFAULT 1 NOT NULL,

    -- When this config row was inserted. Auto-populated by Oracle.
    created_at       TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,

    -- Guard against misconfiguration: zero or negative values would break
    -- the window walk algorithm (infinite loop or division by zero).
    CONSTRAINT chk_max_per_window CHECK (max_per_window > 0)
);

-- Speeds up the hot-path query: loading the active config by name.
-- Used by RateLimitConfigRepository.loadActiveConfig(configName).
CREATE INDEX idx_config_name_active ON rate_limit_config (config_name, is_active);

-- ============================================================================
-- TABLE: rate_limit_window_counter
-- ============================================================================
-- Lightweight concurrency control table. One row per time window. Acts as a
-- semaphore: the slot_count integer tracks how many events have been assigned
-- to this window, regardless of which config version was active at the time
-- (config-agnostic).
--
-- This table is the lock target for SELECT FOR UPDATE NOWAIT. Keeping it
-- separate from the event slot table is critical for performance:
--   - Locking a single counter row is O(1), vs. locking N event rows.
--   - Lock hold time stays constant regardless of how full the window is.
--   - The NOWAIT skip-to-next-window pattern requires exactly one lock point
--     per window to produce a clear Contended/Full/Available signal.
--
-- Rows are created lazily on first event per window (INSERT + catch ORA-00001).
-- Can be archived aggressively once windows are no longer assignment candidates.
-- ============================================================================
CREATE TABLE rate_limit_window_counter (
    -- The aligned start timestamp of the time window.
    -- Windows are aligned to epoch: windowStart = requestedTime - (requestedTime % windowSizeSecs).
    -- This is the PRIMARY KEY and the target of SELECT FOR UPDATE NOWAIT.
    window_start TIMESTAMP NOT NULL,

    -- Number of events assigned to this window so far. Incremented after each
    -- successful INSERT into rate_limit_event_slot. Compared against
    -- max_per_window to determine if the window is full.
    -- Only incremented after the event slot row is confirmed inserted, so it
    -- is never inflated by failed idempotency-race inserts.
    slot_count   NUMBER(10) DEFAULT 0 NOT NULL,

    -- Denormalized availability flag. OPEN = window has capacity, CLOSED = full.
    -- Set atomically with slot_count increment: when slot_count reaches
    -- max_per_window, status flips to CLOSED. Enables a fast indexed skip query
    -- to find the first available window without scanning full windows.
    status       VARCHAR2(10) DEFAULT 'OPEN' NOT NULL,

    CONSTRAINT pk_window_counter PRIMARY KEY (window_start)
);

-- Composite index for the skip query: find first OPEN window after a given timestamp.
-- Used by find_first_open_window() in the PL/SQL block.
CREATE INDEX idx_window_status_start ON rate_limit_window_counter(status, window_start);

-- ============================================================================
-- TABLE: rate_limit_event_slot
-- ============================================================================
-- Immutable audit record of every slot assignment. One row per event. Once
-- inserted, rows are never updated or deleted by the application.
--
-- Serves multiple purposes:
--   - Idempotency: the UNIQUE constraint on event_id ensures that calling
--     assignSlot() twice with the same event_id returns the same slot.
--   - Audit trail: records which window and scheduled_time each event received,
--     which config was active, and when the assignment happened.
--   - Reconciliation: used to detect slot leakage (events assigned a slot but
--     never acted upon by the caller) by querying recent unprocessed slots.
--
-- This table is NOT locked during assignment — only the window_counter row is.
-- Events are the "data" protected by the counter semaphore.
-- ============================================================================
-- Sequence for rate_limit_event_slot primary key.
CREATE SEQUENCE rate_limit_event_slot_slot_id_seq START WITH 1 INCREMENT BY 1 NOCACHE;

CREATE TABLE rate_limit_event_slot (
    -- Surrogate primary key, auto-generated via sequence.
    slot_id        NUMBER(19) DEFAULT rate_limit_event_slot_slot_id_seq.NEXTVAL PRIMARY KEY,

    -- Caller-provided unique event identifier. Acts as the idempotency key.
    -- If two concurrent threads try to insert the same event_id, Oracle raises
    -- ORA-00001 on the UNIQUE constraint, and the loser re-reads the winner's row.
    event_id       VARCHAR2(256)  NOT NULL,

    -- When the caller originally requested this event to be processed.
    -- Captured at INSERT time as Instant.now(). Used for auditing how far
    -- the actual scheduled_time diverged from the request.
    requested_time TIMESTAMP      NOT NULL,

    -- The time window this event was assigned to. Matches a row in
    -- rate_limit_window_counter. Multiple events share the same window_start.
    window_start   TIMESTAMP      NOT NULL,

    -- The actual execution time assigned to this event, computed as:
    --   windowStart + random(0, windowSizeMs)
    -- Returned to the caller as part of the AssignedSlot response.
    -- Random jitter ensures uniform distribution within the window.
    scheduled_time TIMESTAMP      NOT NULL,

    -- The rate_limit_config.config_id that was active when this slot was assigned.
    -- Stored for audit purposes — allows reconstructing which max_per_window
    -- and window_size were in effect for this event.
    -- No foreign key constraint: the application guarantees validity by loading
    -- the config before inserting, and decoupling avoids FK check overhead on
    -- every insert.
    config_id      NUMBER(19)     NOT NULL,

    -- When this slot was assigned. Auto-populated by Oracle.
    -- Used by the reconciliation query to find potentially leaked slots
    -- (e.g., WHERE created_at < SYSDATE - INTERVAL '10' MINUTE).
    created_at     TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,

    -- Idempotency enforcement. Prevents duplicate slot assignments for the
    -- same event under concurrent access. On violation (ORA-00001), the
    -- application re-reads the existing row instead of inserting.
    CONSTRAINT uq_event_id UNIQUE (event_id)
);

-- Speeds up queries that filter by window (e.g., reconciliation, counter
-- consistency checks in tests).
CREATE INDEX idx_event_slot_window  ON rate_limit_event_slot (window_start);

-- Speeds up the reconciliation query that finds recently assigned slots.
CREATE INDEX idx_event_slot_created ON rate_limit_event_slot (created_at);
