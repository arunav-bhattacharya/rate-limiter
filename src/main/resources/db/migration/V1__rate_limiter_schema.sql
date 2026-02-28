-- ============================================================================
-- TABLE: RL_EVENT_WNDW_CONFIG
-- ============================================================================
-- Stores rate limit configuration parameters. Supports dynamic updates via
-- versioned rows: inserting a new config deactivates the previous one.
-- The application caches the active config in memory (5s TTL) and loads
-- from this table on cache miss.
--
-- Multiple config names can coexist (e.g., "default", "high-priority") but
-- only one row per WNDW_CONFIG_NM should have ACT_IN = 1 at any time.
-- Old rows are kept for audit; never delete — deactivate instead.
-- ============================================================================
CREATE TABLE RL_EVENT_WNDW_CONFIG
(
    RL_WNDW_CONFIG_ID    VARCHAR2(50)  NOT NULL
        CONSTRAINT RL_EVENT_WNDW_CONFIG_PK PRIMARY KEY,
    WNDW_CONFIG_NM       VARCHAR2(128) NOT NULL,
    WNDW_MAX_EVENT_CT    NUMBER(10)    NOT NULL,
    WNDW_SIZE_ISO_DUR_TX VARCHAR2(25)  NOT NULL,
    CONFIG_EFF_STRT_DT   TIMESTAMP(6)  NOT NULL,
    ACT_IN               NUMBER(1)     NOT NULL,
    CREAT_TS             TIMESTAMP(6)  NOT NULL
);

-- Speeds up the hot-path query: loading the active config by name.
CREATE INDEX RL_EVENT_WNDW_CONFIG_I01X ON RL_EVENT_WNDW_CONFIG (WNDW_CONFIG_NM, ACT_IN);

-- ============================================================================
-- TABLE: RL_WNDW_CT
-- ============================================================================
-- Lightweight concurrency control table. One row per time window. Acts as a
-- semaphore: the SLOT_CT integer tracks how many events have been assigned
-- to this window, regardless of which config version was active at the time
-- (config-agnostic).
--
-- This table is the lock target for SELECT FOR UPDATE SKIP LOCKED. Keeping it
-- separate from the event slot table is critical for performance:
--   - Locking a single counter row is O(1), vs. locking N event rows.
--   - Lock hold time stays constant regardless of how full the window is.
--   - The SKIP LOCKED skip-to-next-window pattern requires exactly one lock
--     point per window to produce a clear Contended/Full/Available signal.
--
-- Rows are created lazily on first event per window (INSERT + catch ORA-00001).
-- Can be archived aggressively once windows are no longer assignment candidates.
-- ============================================================================
CREATE TABLE RL_WNDW_CT
(
    WNDW_STRT_TS TIMESTAMP(6)         NOT NULL
        CONSTRAINT RL_WNDW_CT_PK PRIMARY KEY,
    SLOT_CT      NUMBER(10) DEFAULT 0 NOT NULL,
    CREAT_TS     TIMESTAMP(6)         NOT NULL
);

-- Composite index for skip/find queries on window counters.
CREATE INDEX RL_WNDW_CT_I01X ON RL_WNDW_CT (WNDW_STRT_TS, SLOT_CT);

-- ============================================================================
-- TABLE: RL_WNDW_FRONTIER_TRK
-- ============================================================================
-- Append-only frontier tracker for provisioned window ranges.
-- Allows multiple frontier rows per REQ_TS (one per extension).
-- Read via SELECT MAX(WNDW_END_TS); write via INSERT only — no UPDATE contention.
-- ============================================================================
CREATE TABLE RL_WNDW_FRONTIER_TRK
(
    REQ_TS      TIMESTAMP(6) NOT NULL,
    WNDW_END_TS TIMESTAMP(6) NOT NULL,
    CREAT_TS    TIMESTAMP(6) NOT NULL,
    CONSTRAINT RL_WNDW_FRONTIER_TRK_PK PRIMARY KEY (REQ_TS, WNDW_END_TS)
);

-- ============================================================================
-- TABLE: RL_EVENT_SLOT_DTL
-- ============================================================================
-- Immutable audit record of every slot assignment. One row per event. Once
-- inserted, rows are never updated or deleted by the application.
--
-- Serves multiple purposes:
--   - Idempotency: the UNIQUE index on EVENT_ID ensures that calling
--     assignSlot() twice with the same EVENT_ID returns the same slot.
--   - Audit trail: records which window and COMPUTED_SCHED_TS each event
--     received, which config was active, and when the assignment happened.
--   - Reconciliation: used to detect slot leakage (events assigned a slot but
--     never acted upon by the caller) by querying recent unprocessed slots.
--
-- This table is NOT locked during assignment — only the RL_WNDW_CT row is.
-- Events are the "data" protected by the counter semaphore.
-- ============================================================================
CREATE TABLE RL_EVENT_SLOT_DTL
(
    WNDW_SLOT_ID      VARCHAR2(50) NOT NULL
        CONSTRAINT RL_EVENT_SLOT_DTL_PK PRIMARY KEY,
    EVENT_ID          VARCHAR2(50) NOT NULL,
    REQ_TS            TIMESTAMP(6) NOT NULL,
    RL_WNDW_CONFIG_ID VARCHAR2(50) NOT NULL,
    WNDW_STRT_TS      TIMESTAMP(6) NOT NULL,
    COMPUTED_SCHED_TS TIMESTAMP(6) NOT NULL,
    CREAT_TS          TIMESTAMP(6) NOT NULL
);

-- Unique index on EVENT_ID for idempotency enforcement.
CREATE UNIQUE INDEX RL_EVENT_SLOT_DTL_IUX ON RL_EVENT_SLOT_DTL (EVENT_ID);

-- Speeds up queries that filter by window.
CREATE INDEX RL_EVENT_SLOT_DTL_I01X ON RL_EVENT_SLOT_DTL (WNDW_STRT_TS);

-- Speeds up the reconciliation query that finds recently assigned slots.
CREATE INDEX RL_EVENT_SLOT_DTL_I02X ON RL_EVENT_SLOT_DTL (CREAT_TS);
