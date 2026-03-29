# SlotAssignmentServiceV6 — Async Counter + Soft Guard

## Design Rationale

V5's hot path does INSERT slot + UPDATE counter (RETURNING INTO) + rollback if maxSlots exceeded, all in one transaction. At high TPS, every concurrent claim serializes on the same counter row in `RL_WNDW_CT`. V6 eliminates this by moving counter updates to a background scheduler and replacing the atomic maxSlots check with a soft guard.

**Trade-off:** Accept bounded over-allocation (configurable `overflowBuffer`) in exchange for zero counter-table contention in the hot path.

## Architecture Change

| Aspect | V5 | V6 |
|--------|----|----|
| Hot path DB writes | INSERT slot + UPDATE counter | INSERT slot only |
| Counter updates | Atomic in hot path (RETURNING INTO) | Background scheduler (MERGE via CREAT_TS) |
| Capacity enforcement | Atomic check + rollback + retry | Soft guard (fresh COUNT before INSERT) |
| Over-allocation | Zero (atomic) | Bounded by `overflowBuffer` (~0.55% at 500 TPS) |
| Counter contention | Every claim serializes on counter row | Zero in hot path |
| Rollback rate | Non-zero (maxSlots exceeded) | Zero |
| DB calls per request | 3: skip ptr + occupancy + (INSERT+UPDATE) | 4: skip ptr + occupancy + COUNT(*) + INSERT |

**Net effect:** Trades 1 extra read-only DB call for eliminating all write contention and rollbacks.

## Hot Path Flow

```
assignSlot(eventId, requestedTime, maxDuration)
│
├─ Skip pointer read          (PK lookup on RL_SKIP_PTR)
│
├─ Phase 1: Normal (softMax, chunked)
│  ├─ Stale occupancy read    (range scan on RL_WNDW_CT PK, one chunk)
│  ├─ Proximity-weighted pick
│  ├─ Soft guard COUNT(*)     (index scan on RL_EVENT_SLOT_DTL, single window)
│  └─ INSERT slot             (single row into RL_EVENT_SLOT_DTL)
│
├─ Phase 2: Overflow (maxSlotsPerWindow, fresh read from requestedTime)
│
└─ Phase 3: Extension (softMax, beyond maxDuration, chunked)
```

**DB calls per request (happy path): 4**
1. Skip pointer read — PK lookup on `RL_SKIP_PTR`
2. Advisory occupancy read — range scan on `RL_WNDW_CT` PK (one chunk, stale OK)
3. Soft guard `COUNT(*)` — index scan on `RL_EVENT_SLOT_DTL` (single window)
4. Slot INSERT — single row into `RL_EVENT_SLOT_DTL`

## Soft Guard

Before INSERT, a fresh `COUNT(*)` on `RL_EVENT_SLOT_DTL` for the picked window. If `freshCount >= absoluteCeiling`, the window is rejected and another is picked from remaining candidates.

```
softMax          = floor(maxSlots * softMaxPercent / 100)   — Phase 1 operating limit
absoluteCeiling  = maxSlotsPerWindow + overflowBuffer        — soft guard hard limit
```

The soft guard runs in its own short-lived transaction (separate from INSERT) so it sees committed data from other pods.

## Background Scheduler

`WindowCounterRefreshScheduler` reconciles `RL_WNDW_CT` counters with actual slot counts from `RL_EVENT_SLOT_DTL`.

### CREAT_TS-Based Discovery

Instead of scanning a fixed time range (which would need to cover 30+ days = 86,400+ windows), the scheduler asks: "which windows received new slots recently?" It uses the `RL_EVENT_SLOT_DTL_I02X(CREAT_TS)` index to find recently inserted slots, extracts distinct windows, then counts ALL slots for just those windows.

```sql
MERGE INTO RL_WNDW_CT tgt
USING (
    SELECT d.WNDW_STRT_TS, COUNT(*) cnt
    FROM RL_EVENT_SLOT_DTL d
    WHERE d.WNDW_STRT_TS IN (
        SELECT DISTINCT WNDW_STRT_TS
        FROM RL_EVENT_SLOT_DTL
        WHERE CREAT_TS >= :since
    )
    GROUP BY d.WNDW_STRT_TS
) src ON (tgt.WNDW_STRT_TS = src.WNDW_STRT_TS)
WHEN MATCHED THEN UPDATE SET SLOT_CT = src.cnt
WHEN NOT MATCHED THEN INSERT (WNDW_STRT_TS, SLOT_CT, CREAT_TS)
    VALUES (src.WNDW_STRT_TS, src.cnt, SYSTIMESTAMP)
```

**Why this works for 30-day spread:** A request with `requestedTime = now + 25 days` inserts a slot with `CREAT_TS = now`. The scheduler finds it via `CREAT_TS >= since`, extracts the window at day 25, and counts it.

### Multi-Pod Coordination

With N pods (e.g., 20), N independent `@Scheduled` timers fire at staggered times.

- **Effective refresh rate:** N / interval = 20 / 3s = one refresh every ~150ms
- **Contention:** Pods rarely overlap; when they do, writes are idempotent (SET, not INCREMENT)
- **No locking needed:** Last writer wins, and last writer has the freshest data

| Source | Writes/sec on counter rows | Nature |
|--------|---------------------------|--------|
| V5 hot path | 500 competing increments | Conflicting |
| V6 scheduler (20 pods) | ~7 non-overlapping SETs | Non-conflicting |

## Configuration

### Production
```yaml
rate-limiter:
  v6:
    window-size: 30s
    max-slots-per-window: 900
    soft-max-percent: 90
    default-max-duration: 8h
    window-chunk-duration: 15m
    extension-windows: 40
    max-extensions-beyond: 5
    overflow-buffer: 20
    counter-refresh-every: 3s
    counter-refresh-since: 6s
```

### Test
```yaml
rate-limiter:
  v6:
    window-size: 4s
    max-slots-per-window: 4
    soft-max-percent: 75
    default-max-duration: 1h
    window-chunk-duration: 16s
    extension-windows: 4
    max-extensions-beyond: 3
    overflow-buffer: 1
    counter-refresh-every: 1s
    counter-refresh-since: 2s
```

## Over-Allocation Analysis (500 TPS, 20 pods)

| Metric | Value |
|--------|-------|
| Effective refresh interval | ~150ms (3s / 20 pods) |
| Events between effective ticks | 75 (500 × 0.15) |
| Phase 1 chunk windows | 30 |
| Hottest window share (proximity-weighted) | ~6.5% |
| Max stale drift on hottest window | ~5 events |
| Worst-case overshoot | 905/900 = 0.55% |
| Absolute ceiling (soft guard) | 920 (900 + 20) |

## Key Files

| File | Role |
|------|------|
| `SlotAssignmentServiceV6.kt` | Core algorithm — three-phase with soft guard |
| `WindowCounterRefreshScheduler.kt` | Background counter reconciliation |
| `EventSlotRepository.kt` | `countSlotsInWindow()` for soft guard |
| `WindowSlotCounterRepository.kt` | `refreshRecentlyActiveCounters()` for scheduler |
