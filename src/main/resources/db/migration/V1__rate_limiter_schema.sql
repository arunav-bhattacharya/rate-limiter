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

    CREATE TABLE rate_limit_config
    (
        config_id      NUMBER(19) DEFAULT rate_limit_config_config_id_seq.NEXTVAL PRIMARY KEY,
        config_name    VARCHAR2(128)  NOT NULL,
        max_per_window NUMBER(10)     NOT NULL,
        window_size    VARCHAR2(50)   NOT NULL,
        effective_from TIMESTAMP                      NOT NULL,
        is_active      NUMBER(1)      DEFAULT 1 NOT NULL,
        created_at     TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
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
    CREATE TABLE rate_limit_window_counter
    (
        window_start TIMESTAMP NOT NULL,
        slot_count   NUMBER(10) DEFAULT 0 NOT NULL,
        CONSTRAINT pk_window_counter PRIMARY KEY (window_start)
    );

    -- Composite index for the skip query: find first non-full window after a given timestamp.
    -- Used by find_first_available_window() in the PL/SQL block.
    -- Supports: WHERE slot_count < max_per_window AND window_start > start_ts
    CREATE INDEX idx_window_counter_slot_start ON rate_limit_window_counter (slot_count, window_start);

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

    CREATE TABLE rate_limit_event_slot
    (
        slot_id        NUMBER(19) DEFAULT rate_limit_event_slot_slot_id_seq.NEXTVAL PRIMARY KEY,
        event_id       VARCHAR2(256)  NOT NULL,
        requested_time TIMESTAMP                      NOT NULL,
        window_start   TIMESTAMP                      NOT NULL,
        scheduled_time TIMESTAMP                      NOT NULL,
        config_id      NUMBER(19)     NOT NULL,
        created_at     TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
        CONSTRAINT uq_event_id UNIQUE (event_id)
    );

    -- Speeds up queries that filter by window (e.g., reconciliation, counter
    -- consistency checks in tests).
    CREATE INDEX idx_event_slot_window ON rate_limit_event_slot (window_start);

    -- Speeds up the reconciliation query that finds recently assigned slots.
    CREATE INDEX idx_event_slot_created ON rate_limit_event_slot (created_at);

    -- ============================================================================
    -- TABLE: track_window_end
    -- ============================================================================
    -- Append-only frontier tracker for provisioned window ranges.
    -- Allows multiple frontier rows per requested_time (one per extension).
    -- Read via SELECT MAX(window_end); write via INSERT only — no UPDATE contention.
    -- ============================================================================
    CREATE TABLE track_window_end
    (
        requested_time TIMESTAMP NOT NULL,
        window_end     TIMESTAMP NOT NULL,
        CONSTRAINT pk_window_start PRIMARY KEY (requested_time, window_end)
    );
