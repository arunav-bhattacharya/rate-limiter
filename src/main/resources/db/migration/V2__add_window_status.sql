-- ============================================================================
-- V2: Add WNDW_STATUS column to RL_WNDW_CT for V7 slot assignment
-- ============================================================================
-- Tracks window occupancy status: 'AVAILABLE' (can accept slots) or 'FULL'
-- (at max capacity). Updated by the background counter refresh scheduler.
-- Hot-path queries filter on this column to skip full windows.
-- ============================================================================

ALTER TABLE RL_WNDW_CT ADD WNDW_STATUS VARCHAR2(10) DEFAULT 'AVAILABLE' NOT NULL;

-- Composite index for V7 hot-path query:
-- WHERE WNDW_STATUS = 'AVAILABLE' AND WNDW_STRT_TS >= ? AND WNDW_STRT_TS < ?
CREATE INDEX RL_WNDW_CT_I02X ON RL_WNDW_CT (WNDW_STATUS, WNDW_STRT_TS);
