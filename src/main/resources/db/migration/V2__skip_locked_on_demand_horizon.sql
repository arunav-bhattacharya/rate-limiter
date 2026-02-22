-- ============================================================================
-- V2: SKIP LOCKED on-demand horizon support + config headroom override
-- ============================================================================

-- Optional per-config headroom override.
ALTER TABLE rate_limit_config
    ADD headroom_windows NUMBER(10);

ALTER TABLE rate_limit_config
    ADD CONSTRAINT chk_headroom_windows_positive
    CHECK (headroom_windows IS NULL OR headroom_windows > 0);

-- Shared on-demand horizon state for window row materialization.
CREATE TABLE rate_limit_horizon_state (
    horizon_key      VARCHAR2(32) PRIMARY KEY,
    horizon_start    TIMESTAMP    NOT NULL,
    horizon_end      TIMESTAMP    NOT NULL,
    window_size_secs NUMBER(10),
    updated_at       TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);

INSERT INTO rate_limit_horizon_state(horizon_key, horizon_start, horizon_end, window_size_secs, updated_at)
VALUES ('GLOBAL', TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '1970-01-01 00:00:00', NULL, SYSTIMESTAMP);

-- Supports Phase 2 eligibility scan ordered by window_start with slot_count predicate.
CREATE INDEX idx_window_start_count ON rate_limit_window_counter(window_start, slot_count);
