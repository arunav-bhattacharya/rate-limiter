# SlotAssignmentServiceV4

Lock-free, counter-free slot assignment. Uses conditional INSERT + blocklist queries instead of `FOR UPDATE SKIP LOCKED` and the `RL_WNDW_CT` counter table.

## How It Differs from V3

| Aspect | V3 | V4 |
|--------|----|----|
| Window locking | `FOR UPDATE SKIP LOCKED` on `RL_WNDW_CT` | None — no row locks |
| Counter table | `RL_WNDW_CT` incremented atomically | Not used — capacity derived from `COUNT(*)` on `RL_EVENT_SLOT_DTL` |
| Window selection | Sequential scan (first available) | Random pick from available windows |
| Provisioning | Batch-provision chunk of counter rows + frontier tracking | No provisioning needed — windows are implicit |
| Capacity model | Exact (`SLOT_CT < max`) | Soft threshold (`COUNT(*) >= floor(max * fillThreshold)`) — over-capacity tolerated |
| Capacity scope | Per-config | Global across all `requestedTime` values (shared windows) |
| Concurrency strategy | Row-level locks prevent double-assignment | Optimistic — concurrent threads may target the same window; `UNIQUE(EVENT_ID)` provides idempotency |

## Algorithm

```
assignSlot(eventId, requestedTime)
│
├── 1. Idempotency check (own transaction)
│   └── fetchAssignedSlot(eventId) → return if exists
│
├── 2. Compute scan boundaries
│   ├── currMaxWindow = fetchMaxWindowStart(requestedTime)
│   │   → furthest window with any slot for this requestedTime (null if no slots exist)
│   │
│   ├── initialEndWindow = max(requestedTime, currMaxWindow + windowSize)
│   │   → end of the "known" range (windows that already have slots)
│   │
│   ├── totalInInitialRange = (initialEndWindow - requestedTime) / windowSize
│   │
│   ├── fullWindowsInInitialRange = findFullWindowsInRange(requestedTime, initialEndWindow, softMax)
│   │   → set of windows where COUNT(*) >= softMax (global, not per-requestedTime)
│   │
│   ├── availableInInitialRange = totalInInitialRange - fullWindowsInInitialRange.size
│   │
│   └── computedEndWindow =
│       ├── if available >= headroomWindows * headroomCapacityThreshold → initialEndWindow (no extension)
│       └── else → initialEndWindow + headroomWindows * windowSize (extend)
│
├── 3. Build available windows
│   └── All windows in [requestedTime, computedEndWindow) minus fullWindowsInInitialRange
│       Windows beyond initialEndWindow have no slots → all available
│
└── 4. Claim slot
    ├── Random pick from available windows
    ├── Random jitter within picked window → scheduledTime
    └── insertAndReturnSlot (INSERT with UNIQUE(EVENT_ID) idempotency)
```

## Database Interactions

V4 uses only the `RL_EVENT_SLOT_DTL` table. No counter table, no frontier table.

| Step | Query | Transaction |
|------|-------|-------------|
| Idempotency check | `SELECT ... WHERE EVENT_ID = ?` | Own |
| Fetch max window | `SELECT MAX(WNDW_STRT_TS) WHERE REQ_TS = ?` | Own |
| Find full windows | `SELECT WNDW_STRT_TS ... WHERE range ... GROUP BY ... HAVING COUNT(*) >= softMax` | Own |
| Claim slot | `INSERT INTO RL_EVENT_SLOT_DTL ...` (duplicate key → re-read) | Own |

Each query runs in its own short-lived transaction.

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `rate-limiter.max-per-window` | 100 | Hard capacity limit per window |
| `rate-limiter.window-size-seconds` | 30 | Window duration in seconds |
| `rate-limiter.headroom-windows` | 100 | Number of windows to extend by when capacity is low |
| `rate-limiter.window-fill-threshold` | 0.9 | Soft fill ratio — windows with `COUNT(*) >= floor(max * threshold)` are treated as full |
| `rate-limiter.headroom-capacity-threshold` | 0.5 | Fraction of headroom that must be available in initial range to skip extension |

### Derived Values

| Value | Formula | Default |
|-------|---------|---------|
| `softMaxSlots` | `floor(maxPerWindow * windowFillThreshold)` | 90 |
| `windowSize` | `Duration.ofSeconds(windowSizeSeconds)` | 30s |
| Extension trigger | `availableInInitialRange < headroomWindows * headroomCapacityThreshold` | < 50 windows |

---

## Scenarios

All scenarios use the following config for readability:

| Parameter | Value |
|-----------|-------|
| `maxPerWindow` | 5 |
| `windowSizeSeconds` | 60 (1 minute) |
| `headroomWindows` | 4 |
| `windowFillThreshold` | 0.8 |
| `headroomCapacityThreshold` | 0.5 |

**Derived**: `softMaxSlots = floor(5 * 0.8) = 4`, extension threshold = `4 * 0.5 = 2` available windows needed.

Window labels used in examples:

| Label | WNDW_STRT_TS |
|-------|--------------|
| W+0 | requestedTime + 0min |
| W+1 | requestedTime + 1min |
| W+2 | requestedTime + 2min |
| ... | ... |

---

### Scenario 1: Empty Table, First Request

**Request**: `assignSlot("evt-1", 14:00:00)`

**RL_EVENT_SLOT_DTL before**: _(empty)_

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| `currMaxWindow` | `null` | No rows with `REQ_TS = 14:00:00` |
| `initialEndWindow` | `14:00:00` | `max(14:00:00, null ?: 14:00:00)` = `14:00:00` |
| `totalInInitialRange` | `0` | `14:00:00 > 14:00:00`? No → 0 |
| `fullWindowsInInitialRange` | `{}` | Range `[14:00:00, 14:00:00)` is empty — nothing to query |
| `availableInInitialRange` | `0` | `0 - 0 = 0` |
| Extension? | **Yes** | `0 >= 2`? No → extend |
| `computedEndWindow` | `14:04:00` | `14:00:00 + 60s * 4` |
| Available windows | `[W+0, W+1, W+2, W+3]` | `[14:00, 14:01, 14:02, 14:03]` — all available (none in blocklist) |
| Random pick | `W+1` (14:01:00) | _(example)_ |
| Jitter | `23456ms` | Random in `[0, 60000)` |
| `scheduledTime` | `14:01:23.456` | `14:01:00 + 23.456s` |

**RL_EVENT_SLOT_DTL after**:

| EVENT_ID | REQ_TS | WNDW_STRT_TS | COMPUTED_SCHED_TS |
|----------|--------|--------------|-------------------|
| evt-1 | 14:00:00 | 14:01:00 | 14:01:23.456 |

**Key point**: With an empty table, the initial range is zero-width. Extension always triggers, opening `headroomWindows` fresh windows.

---

### Scenario 2: Different requestedTime After Existing Slots

**Existing state**: Several slots exist for `requestedTime = 14:00:00`.

**RL_EVENT_SLOT_DTL before**:

| EVENT_ID | REQ_TS | WNDW_STRT_TS | COMPUTED_SCHED_TS |
|----------|--------|--------------|-------------------|
| evt-1 | 14:00:00 | 14:00:00 | 14:00:45.123 |
| evt-2 | 14:00:00 | 14:01:00 | 14:01:12.789 |
| evt-3 | 14:00:00 | 14:01:00 | 14:01:55.321 |

**Request**: `assignSlot("evt-10", 13:00:00)`

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| `currMaxWindow` | `null` | No rows with `REQ_TS = 13:00:00` |
| `initialEndWindow` | `13:00:00` | No existing slots for this requestedTime |
| `totalInInitialRange` | `0` | Zero-width range |
| `fullWindowsInInitialRange` | `{}` | Range `[13:00:00, 13:00:00)` is empty |
| `availableInInitialRange` | `0` | |
| Extension? | **Yes** | `0 >= 2`? No → extend |
| `computedEndWindow` | `13:04:00` | `13:00:00 + 60s * 4` |
| Available windows | `[13:00, 13:01, 13:02, 13:03]` | No overlap with 14:xx windows |

**RL_EVENT_SLOT_DTL after** (evt-10 randomly assigned to W+2):

| EVENT_ID | REQ_TS | WNDW_STRT_TS | COMPUTED_SCHED_TS |
|----------|--------|--------------|-------------------|
| evt-1 | 14:00:00 | 14:00:00 | 14:00:45.123 |
| evt-2 | 14:00:00 | 14:01:00 | 14:01:12.789 |
| evt-3 | 14:00:00 | 14:01:00 | 14:01:55.321 |
| evt-10 | 13:00:00 | 13:02:00 | 13:02:37.891 |

**Key point**: Each `requestedTime` has independent scan boundaries. `currMaxWindow` is scoped by `REQ_TS`, so 14:00:00 slots don't influence the 13:00:00 scan range. The window ranges don't overlap (13:00-13:04 vs 14:00-14:04), so there's no capacity interaction.

---

### Scenario 3: Bulk Concurrent Requests — Empty Start

**Request**: 20 concurrent calls to `assignSlot("evt-1"..."evt-20", 13:00:00)` when no slots exist for `13:00:00`.

**RL_EVENT_SLOT_DTL before**: _(no rows for REQ_TS = 13:00:00)_

**What each thread sees** (simultaneously, before any inserts commit):

| Step | Value |
|------|-------|
| `currMaxWindow` | `null` |
| `initialEndWindow` | `13:00:00` |
| `totalInInitialRange` | `0` |
| `fullWindowsInInitialRange` | `{}` |
| `availableInInitialRange` | `0` |
| Extension? | **Yes** → `computedEndWindow = 13:04:00` |
| Available windows | `[13:00, 13:01, 13:02, 13:03]` |

Each thread independently picks a random window from the 4 available and inserts. With uniform random distribution, roughly **5 slots per window**.

**RL_EVENT_SLOT_DTL after** (example distribution):

| WNDW_STRT_TS | Slot Count | EVENT_IDs (example) |
|--------------|------------|---------------------|
| 13:00:00 | 6 | evt-2, evt-5, evt-8, evt-11, evt-15, evt-19 |
| 13:01:00 | 4 | evt-1, evt-7, evt-13, evt-17 |
| 13:02:00 | 5 | evt-3, evt-6, evt-10, evt-14, evt-20 |
| 13:03:00 | 5 | evt-4, evt-9, evt-12, evt-16, evt-18 |

**Key points**:
- All 20 inserts succeed — there are no locks to prevent concurrent writes to the same window.
- Window 13:00 has 6 slots, exceeding `softMaxSlots = 4`. This is the **over-capacity tolerance** by design. The soft threshold is only used for future capacity *reads*, not enforced on writes.
- Future requests will see 13:00, 13:02, 13:03 as "full" (count >= 4) and avoid them.

---

### Scenario 4: Bulk Requests with Pre-Existing Slots

**Request**: 10 concurrent calls to `assignSlot("evt-30"..."evt-39", 14:00:00)`.

**RL_EVENT_SLOT_DTL before** (from earlier requests for `14:00:00`):

| WNDW_STRT_TS | Slot Count | Status (vs softMax=4) |
|--------------|------------|-----------------------|
| 14:00:00 | 4 | Full |
| 14:01:00 | 3 | Available (1 remaining) |
| 14:02:00 | 4 | Full |

**What each thread sees** (simultaneously):

| Step | Value | Reasoning |
|------|-------|-----------|
| `currMaxWindow` | `14:02:00` | Furthest `WNDW_STRT_TS` where `REQ_TS = 14:00:00` |
| `initialEndWindow` | `14:03:00` | `max(14:00:00, 14:02:00 + 60s)` |
| `totalInInitialRange` | `3` | `(14:03:00 - 14:00:00) / 60s` |
| `fullWindowsInInitialRange` | `{14:00, 14:02}` | Windows with `COUNT(*) >= 4` |
| `availableInInitialRange` | `1` | `3 - 2` |
| Extension? | **Yes** | `1 >= 2`? No → extend |
| `computedEndWindow` | `14:07:00` | `14:03:00 + 60s * 4` |
| Available windows | `[14:01, 14:03, 14:04, 14:05, 14:06]` | 14:00 and 14:02 filtered out; 14:03-14:06 are beyond `initialEndWindow` so guaranteed empty |

10 threads randomly pick from 5 windows → roughly **2 per window**.

**RL_EVENT_SLOT_DTL after** (example distribution):

| WNDW_STRT_TS | Before | After | Notes |
|--------------|--------|-------|-------|
| 14:00:00 | 4 | 4 | Stays full — excluded from picks |
| 14:01:00 | 3 | 5 | Was available, got 2 more |
| 14:02:00 | 4 | 4 | Stays full — excluded from picks |
| 14:03:00 | 0 | 2 | New — from extension |
| 14:04:00 | 0 | 2 | New — from extension |
| 14:05:00 | 0 | 2 | New — from extension |
| 14:06:00 | 0 | 2 | New — from extension |

**Key point**: The extension opened 4 fresh windows (14:03-14:06) because the initial range only had 1 available window, below the threshold of 2. The random pick naturally spreads across all 5 eligible windows.

---

### Scenario 5: Enough Available Windows — No Extension

**Setup**: Existing slots for `requestedTime = 14:00:00` spread across 6 windows, with 3 still under capacity.

**RL_EVENT_SLOT_DTL before**:

| WNDW_STRT_TS | Slot Count | Status (vs softMax=4) |
|--------------|------------|-----------------------|
| 14:00:00 | 4 | Full |
| 14:01:00 | 2 | Available |
| 14:02:00 | 4 | Full |
| 14:03:00 | 1 | Available |
| 14:04:00 | 5 | Full |
| 14:05:00 | 3 | Available |

**Request**: `assignSlot("evt-50", 14:00:00)`

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| `currMaxWindow` | `14:05:00` | Furthest window with slots |
| `initialEndWindow` | `14:06:00` | `max(14:00:00, 14:05:00 + 60s)` |
| `totalInInitialRange` | `6` | `(14:06:00 - 14:00:00) / 60s` |
| `fullWindowsInInitialRange` | `{14:00, 14:02, 14:04}` | 3 windows with count >= 4 |
| `availableInInitialRange` | `3` | `6 - 3` |
| Extension? | **No** | `3 >= 2`? Yes → cap at `initialEndWindow` |
| `computedEndWindow` | `14:06:00` | Same as `initialEndWindow` |
| Available windows | `[14:01, 14:03, 14:05]` | Full windows filtered out |

Random pick from `[14:01, 14:03, 14:05]`. No extension was needed because 3 available windows exceeds the threshold of 2.

**Key point**: When the existing range has enough open windows (`available >= headroomWindows * headroomCapacityThreshold`), the scan stays within the known range. This avoids unnecessary spread into far-future windows.

---

### Scenario 6: Few Available Windows — Extension Triggers

**Setup**: Same 6 windows, but now 5 are full.

**RL_EVENT_SLOT_DTL before**:

| WNDW_STRT_TS | Slot Count | Status (vs softMax=4) |
|--------------|------------|-----------------------|
| 14:00:00 | 5 | Full |
| 14:01:00 | 4 | Full |
| 14:02:00 | 4 | Full |
| 14:03:00 | 6 | Full |
| 14:04:00 | 5 | Full |
| 14:05:00 | 2 | Available |

**Request**: `assignSlot("evt-60", 14:00:00)`

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| `currMaxWindow` | `14:05:00` | |
| `initialEndWindow` | `14:06:00` | |
| `totalInInitialRange` | `6` | |
| `fullWindowsInInitialRange` | `{14:00, 14:01, 14:02, 14:03, 14:04}` | 5 full |
| `availableInInitialRange` | `1` | `6 - 5` |
| Extension? | **Yes** | `1 >= 2`? No → extend |
| `computedEndWindow` | `14:10:00` | `14:06:00 + 60s * 4` |
| Available windows | `[14:05, 14:06, 14:07, 14:08, 14:09]` | 14:05 is the only survivor from initial range; 14:06-14:09 are fresh extension windows |

Random pick from 5 windows. The 4 extension windows are guaranteed empty.

**Key point**: Extension triggers when the existing range is nearly exhausted. The `headroomCapacityThreshold` controls how eagerly extension happens — a lower threshold (e.g. 0.2) would wait longer before extending; a higher threshold (e.g. 0.8) would extend more aggressively.

---

### Scenario 7: Early Windows Available, Later Windows Full

**Setup**: The head of the range has capacity, but the tail is packed. This can happen when earlier windows were skipped by random pick and later windows filled up first.

**RL_EVENT_SLOT_DTL before**:

| WNDW_STRT_TS | Slot Count | Status (vs softMax=4) |
|--------------|------------|-----------------------|
| 14:00:00 | 1 | Available |
| 14:01:00 | 2 | Available |
| 14:02:00 | 5 | Full |
| 14:03:00 | 4 | Full |
| 14:04:00 | 6 | Full |
| 14:05:00 | 4 | Full |

**Request**: `assignSlot("evt-70", 14:00:00)`

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| `currMaxWindow` | `14:05:00` | |
| `initialEndWindow` | `14:06:00` | |
| `totalInInitialRange` | `6` | |
| `fullWindowsInInitialRange` | `{14:02, 14:03, 14:04, 14:05}` | 4 full |
| `availableInInitialRange` | `2` | `6 - 4` |
| Extension? | **No** | `2 >= 2`? Yes — just meets threshold |
| `computedEndWindow` | `14:06:00` | No extension |
| Available windows | `[14:00, 14:01]` | Only the early windows survive filtering |

Random pick from `[14:00, 14:01]`. The slot lands in one of the early windows that still have capacity.

**Now, one more request fills 14:01 to its soft limit**:

| WNDW_STRT_TS | Slot Count | Status |
|--------------|------------|--------|
| 14:00:00 | 1 | Available |
| 14:01:00 | 3 | Available |
| 14:02:00-14:05:00 | 4-6 each | Full |

Two more requests fill 14:00 and 14:01 to soft capacity:

| WNDW_STRT_TS | Slot Count | Status |
|--------------|------------|--------|
| 14:00:00 | 4 | Full |
| 14:01:00 | 4 | Full |
| 14:02:00-14:05:00 | 4-6 each | Full |

**Next request after all windows are full**:

| Step | Value | Reasoning |
|------|-------|-----------|
| `currMaxWindow` | `14:05:00` | |
| `initialEndWindow` | `14:06:00` | |
| `totalInInitialRange` | `6` | |
| `fullWindowsInInitialRange` | `{14:00, 14:01, 14:02, 14:03, 14:04, 14:05}` | All 6 full |
| `availableInInitialRange` | `0` | `6 - 6` |
| Extension? | **Yes** | `0 >= 2`? No → extend |
| `computedEndWindow` | `14:10:00` | `14:06:00 + 60s * 4` |
| Available windows | `[14:06, 14:07, 14:08, 14:09]` | All initial range windows filtered out; extension windows are all fresh |

**Key point**: The threshold acts as a tipping point. As long as the early windows keep the available count at or above the threshold, the range stays capped. Once they fill up, extension kicks in. The blocklist only covers `[requestedTime, initialEndWindow)` — extension windows are assumed empty and never queried.

---

### Scenario 8: Idempotent Re-Request

**Request**: `assignSlot("evt-1", 14:00:00)` where `evt-1` was already assigned.

**RL_EVENT_SLOT_DTL before**:

| EVENT_ID | REQ_TS | WNDW_STRT_TS | COMPUTED_SCHED_TS |
|----------|--------|--------------|-------------------|
| evt-1 | 14:00:00 | 14:01:00 | 14:01:23.456 |

**Walkthrough**:

| Step | Value |
|------|-------|
| `fetchAssignedSlot("evt-1")` | Returns `AssignedSlot(evt-1, 14:01:23.456, 1m23.456s)` |

**Result**: Immediately returns the existing slot. No scan, no insert, no capacity queries. Single SELECT by `EVENT_ID`.

**RL_EVENT_SLOT_DTL after**: _(unchanged)_

---

### Scenario 9: Concurrent Duplicate — Race Condition on Same eventId

Two threads simultaneously call `assignSlot("evt-99", 14:00:00)`. Both pass the idempotency check (neither has committed yet).

**Thread A**:
1. `fetchAssignedSlot("evt-99")` → null (not yet inserted)
2. Computes boundaries, picks window 14:02:00
3. `INSERT` → **success** (first to commit)

**Thread B**:
1. `fetchAssignedSlot("evt-99")` → null (Thread A hasn't committed yet)
2. Computes boundaries, picks window 14:03:00
3. `INSERT` → **duplicate key violation** (`UNIQUE(EVENT_ID)`)
4. Catches `ExposedSQLException`, verifies it's a duplicate key
5. `queryAssignedSlot("evt-99")` → returns Thread A's slot

**Result**: Both threads return the same `AssignedSlot`. Only one row exists. The `UNIQUE(EVENT_ID)` constraint guarantees exactly-once semantics.

---

### Scenario 10: Shared Window Capacity Across requestedTimes

`findFullWindowsInRange` counts slots **globally** (not filtered by `REQ_TS`). This means different `requestedTime` values that happen to share the same `WNDW_STRT_TS` compete for capacity.

**Setup**: Two requestedTimes with overlapping window ranges.

**RL_EVENT_SLOT_DTL before**:

| EVENT_ID | REQ_TS | WNDW_STRT_TS | COMPUTED_SCHED_TS |
|----------|--------|--------------|-------------------|
| evt-A1 | 14:00:00 | 14:02:00 | 14:02:15.000 |
| evt-A2 | 14:00:00 | 14:02:00 | 14:02:45.000 |
| evt-A3 | 14:00:00 | 14:02:00 | 14:02:30.000 |
| evt-A4 | 14:00:00 | 14:02:00 | 14:02:55.000 |
| evt-B1 | 14:02:00 | 14:02:00 | 14:02:10.000 |

Window `14:02:00` now has **5 slots total** (4 from `REQ_TS=14:00` + 1 from `REQ_TS=14:02`). `softMaxSlots = 4`.

**Request**: `assignSlot("evt-B2", 14:02:00)`

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| `currMaxWindow` | `14:02:00` | Only window for `REQ_TS = 14:02:00` |
| `initialEndWindow` | `14:03:00` | `max(14:02:00, 14:02:00 + 60s)` |
| `totalInInitialRange` | `1` | `(14:03:00 - 14:02:00) / 60s` |
| `fullWindowsInInitialRange` | `{14:02:00}` | Global `COUNT(*) = 5 >= 4` — includes slots from **both** requestedTimes |
| `availableInInitialRange` | `0` | `1 - 1` |
| Extension? | **Yes** | `0 >= 2`? No → extend |
| `computedEndWindow` | `14:07:00` | `14:03:00 + 60s * 4` |
| Available windows | `[14:03, 14:04, 14:05, 14:06]` | 14:02 filtered out (full); extension windows are fresh |

**Key point**: Even though `REQ_TS=14:02:00` only contributed 1 slot to window `14:02:00`, the window is considered full because the global count (5) exceeds `softMaxSlots` (4). This prevents any single window from being overloaded regardless of which `requestedTime` contributed the slots. The trade-off is that high-volume `requestedTime` values can "crowd out" windows for other `requestedTime` values that share the same range.

---

## Concurrency Model

- **No locks**: Concurrent threads can target the same window. Random window selection distributes load.
- **Idempotency**: `UNIQUE(EVENT_ID)` constraint on `RL_EVENT_SLOT_DTL`. Duplicate insert → catch exception → re-read existing slot.
- **Over-capacity tolerated**: The soft threshold means a window may exceed `softMaxSlots` under concurrency (see Scenario 3). This is by design — the hard limit (`maxPerWindow`) is not enforced at the DB level.
- **No thundering herd**: Random window pick means concurrent threads naturally spread across the available range rather than all contending on the first open window.

## Trade-offs

| Pro | Con |
|-----|-----|
| No row locks → no lock contention | Soft capacity — windows can exceed threshold under concurrency |
| No counter table → simpler schema | `COUNT(*)` queries may be slower than counter lookups at very high slot counts |
| No provisioning step → fewer DB round-trips | Extension decision based on a point-in-time snapshot — may over- or under-extend |
| Random pick → natural load spreading | Non-deterministic window selection — harder to reason about fill order |
| Global capacity counting → prevents window overload | Different requestedTimes sharing windows can crowd each other out |
