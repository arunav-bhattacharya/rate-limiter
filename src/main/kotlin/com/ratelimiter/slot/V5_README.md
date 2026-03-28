# SlotAssignmentServiceV5

Optimistic, lock-free rate-limiter that assigns time slots to events within fixed windows. Designed for multi-pod deployments (20+ pods) handling sustained traffic at 30+ TPS downstream, with per-request control over how far into the future slots can be placed.

---

## What Problem Does This Solve?

Imagine a payment system that can process **30 transactions per second**. When 500,000 payment requests arrive in a burst, they can't all execute at once — the downstream system would be overwhelmed. This service acts as a **traffic shaper**: it assigns each request a specific future time slot, spreading the load evenly across time windows.

```
Without rate limiting:              With V5 rate limiting:

  Requests   Downstream               Requests    V5 Service    Downstream
  ────────   ──────────               ────────    ──────────    ──────────
  ██████████ → 500K/sec  → CRASH!     ██████████ → schedules → ▓▓▓▓ 30/sec
                                                    future    → ▓▓▓▓ 30/sec
                                                    time      → ▓▓▓▓ 30/sec
                                                    slots     → ▓▓▓▓ 30/sec
                                                              → ... (spread
                                                                 over hours)
```

### Key Capabilities

| Capability | Description |
|-----------|-------------|
| **Per-request duration control** | Each caller specifies how far into the future its slot can go (`maxDuration`). A time-sensitive payment might accept 4 hours; a batch job might accept 24 hours. |
| **Three-phase graceful degradation** | Normal → Overflow → Extension. The system tries harder before giving up, and tells the caller exactly what happened. |
| **Multi-pod coordination** | 20 pods share a DB-backed skip pointer. No JVM-local state — any pod can serve any request. |
| **No pre-provisioning** | Windows and counters are created on demand. No background jobs, no wasted storage. |
| **No row locks** | Optimistic inserts with advisory counters. No `FOR UPDATE`, no lock contention, no deadlocks. |

---

## How It Works — The Three Phases

When a request arrives, V5 tries three increasingly aggressive strategies to find a slot:

```
Request arrives: assignSlot(eventId, requestedTime, maxDuration)
│
├─ Read skip pointer from DB (where did previous requests leave off?)
│
├─ PHASE A: Normal Allocation (softMax, chunked)
│  ├─ Scan forward from skip pointer in 15-min chunks
│  ├─ Within each chunk: read occupancy → proximity-weighted pick → claim
│  ├─ If chunk exhausted: advance skip pointer, try next chunk
│  └─ Continues until maxDuration boundary
│      → Returns NORMAL status
│
├─ PHASE B: Overflow Allocation (hardMax, full rescan)
│  ├─ Fresh occupancy read from requestedTime (ignores skip pointer)
│  ├─ Looks for windows between softMax and hardMax
│  └─ Proximity-weighted pick within maxDuration
│      → Returns SOFT_MAX_EXCEEDED status
│
├─ PHASE C: Extension Allocation (softMax, beyond maxDuration)
│  ├─ Extends in chunks beyond maxDuration
│  ├─ Fresh occupancy read per chunk
│  └─ Advances skip pointer as each extension chunk is exhausted
│      → Returns MAX_DURATION_EXCEEDED status
│
└─ All phases exhausted → throw SlotAssignmentException (503)
```

### Why Three Phases?

```
                    softMax (870)         hardMax (990)        capacity (1000)
                        │                     │                     │
Window occupancy: ░░░░░░████████░░░░░░░░░░░░░░█████░░░░░░░░░░░░░░░░█
                  ──────┼─────────────────────┼─────────────────────┼──
                        │                     │                     │
                  Phase A picks here     Phase B picks        Never used
                  (normal operation)     here (overflow)      (safety margin)
```

- **Phase A** keeps windows below `softMax` — normal operating range with headroom for concurrent writes
- **Phase B** uses the gap between `softMax` and `hardMax` as overflow — only when all windows in `maxDuration` have reached `softMax`
- **Phase C** extends beyond `maxDuration` — last resort, caller is notified via `AllocationStatus`

---

## Chunked Scanning — Why Not Scan Everything at Once?

With an 8-hour `maxDuration` and 30-second windows, there are **960 windows** to search. Scanning all at once has two problems:

1. **Large DB read** — occupancy read covers 960 rows instead of ~30
2. **Weak proximity** — proximity weighting across 960 windows spreads probability too thin

V5 chunks Phase A into configurable batches (default: 15 minutes = 30 windows):

```
maxDuration = 8 hours
chunk size  = 15 minutes

            skipTo
              │
              ▼
Phase A:  [chunk 1] → [chunk 2] → [chunk 3] → ... → [chunk 32]
           15 min      15 min      15 min              15 min
              │
              └── Read occupancy for just this chunk (30 windows)
                  Pick from these 30 windows (tight proximity weighting)
                  If exhausted → advance skip pointer → next chunk
```

**Result**: The first chunk (closest to `requestedTime`) is tried first. If it has capacity, the slot lands within 15 minutes of the requested time — not scattered across 8 hours.

---

## The Skip Pointer — Multi-Pod Coordination

With 20 pods processing requests concurrently, each pod needs to know where to start searching. Without coordination, all 20 pods would re-scan already-full chunks on every request.

The **skip pointer** (`RL_SKIP_PTR` table) tracks the furthest exhausted boundary per `requestedTime`:

```
Pod 1 processes request → exhausts chunk [14:00, 14:15) → writes skipTo = 14:15
Pod 2 processes request → reads skipTo = 14:15 → starts from 14:15 (skips chunk 1)
Pod 3 processes request → exhausts chunk [14:15, 14:30) → writes skipTo = 14:30
Pod 4 processes request → reads skipTo = 14:30 → starts from 14:30 (skips chunks 1-2)

Timeline:
  14:00        14:15        14:30        14:45
    │────────────│────────────│────────────│
    │  EXHAUSTED │  EXHAUSTED │  AVAILABLE │
    │  (skip)    │  (skip)    │  ← start   │
    │            │            │    here     │
```

### Monotonic Advancement

The skip pointer only moves forward, never backward:

```sql
UPDATE RL_SKIP_PTR
SET SKIP_TO_TS = :newSkipTo, UPD_TS = :now
WHERE REQ_TS = :requestedTime AND SKIP_TO_TS < :newSkipTo
```

If two pods try to advance simultaneously, the highest value wins. A pod with a lower value simply no-ops.

---

## Proximity-Weighted Random Selection

V5 doesn't pick the first available window (sequential) or a purely random window (uniform). It uses **proximity-weighted random selection**: windows closer to the start of the range AND with more remaining capacity are more likely to be chosen.

```
Weight(window) = capacityWeight * proximityWeight

  capacityWeight = max(0, threshold - currentOccupancy)
  proximityWeight = rangeSize - index     (linear decay from start)

Example: 4 windows, softMax = 870

  Window    Occupancy   capacityWeight   proximityWeight   totalWeight
  ──────    ─────────   ──────────────   ───────────────   ───────────
  W+0       200         670              4                 2680  ████████████
  W+1       500         370              3                 1110  █████
  W+2       0           870              2                 1740  ████████
  W+3       800         70               1                 70    ▏

  Selection probability:
  W+0: 48%  ████████████████████████
  W+1: 20%  ██████████
  W+2: 31%  ███████████████▌
  W+3:  1%  ▌
```

**Why not first-available?** Under high concurrency, all pods would target the same window, causing contention. Weighted random naturally spreads load while still favoring closer windows.

**Why not uniform random?** A uniform pick ignores proximity entirely — a slot could land 7 hours from now even when nearby windows are empty.

---

## Hard Cap Enforcement

V5 uses a **two-tier capacity model**:

```
  softMax (870)                    hardMax (990)
      │                                │
      ▼                                ▼
  ┌───────────────────────────────────────────────┐
  │░░░░░░░░░░░░░░░░░░░░│▓▓▓▓▓▓▓▓▓▓▓▓│           │
  │  Phase A territory  │  Phase B    │  Reserved │
  │  (normal)           │  (overflow) │  (safety) │
  └───────────────────────────────────────────────┘
  0                                           1000
```

The hard cap is enforced atomically using Oracle's `RETURNING INTO`:

```sql
BEGIN
  UPDATE RL_WNDW_CT SET SLOT_CT = SLOT_CT + 1
  WHERE WNDW_STRT_TS = :windowStart
  RETURNING SLOT_CT INTO :newCount;
END;
```

If `newCount > hardMax`, the entire transaction (slot INSERT + counter UPDATE) is rolled back and a different window is tried. This ensures no window ever exceeds `hardMax`, even under high concurrency.

---

## Idempotency

V5 guarantees exactly-once slot assignment per `eventId` through two layers:

1. **`UNIQUE(EVENT_ID)` constraint** on `RL_EVENT_SLOT_DTL` — the primary guard
2. **Duplicate-key catch** — on constraint violation, re-reads the existing slot within the same transaction

```
Thread A: INSERT eventId="pay-123" → SUCCESS → returns slot
Thread B: INSERT eventId="pay-123" → UNIQUE violation → catches → re-reads A's slot → returns same slot

Result: Both threads return identical AssignedSlot. One row in DB.
```

No upfront idempotency check is needed — the `UNIQUE` constraint handles it with zero extra DB calls on the happy path.

---

## Database Schema

V5 uses three tables:

```
┌─────────────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│  RL_EVENT_SLOT_DTL  │     │     RL_WNDW_CT      │     │   RL_SKIP_PTR    │
│─────────────────────│     │─────────────────────│     │──────────────────│
│ WNDW_SLOT_ID    PK  │     │ WNDW_STRT_TS    PK  │     │ REQ_TS       PK  │
│ EVENT_ID      UQ    │     │ SLOT_CT             │     │ SKIP_TO_TS       │
│ REQ_TS              │     │ CREAT_TS            │     │ UPD_TS           │
│ WNDW_STRT_TS        │     └─────────────────────┘     └──────────────────┘
│ COMPUTED_SCHED_TS   │       Advisory counter            Skip pointer
│ RL_WNDW_CONFIG_ID   │       (created on demand)         (monotonic advance)
│ CREAT_TS            │
└─────────────────────┘
  Immutable slot record
  (idempotency via UQ)
```

| Table | Purpose | Write Pattern |
|-------|---------|---------------|
| `RL_EVENT_SLOT_DTL` | Immutable slot assignments | INSERT only (never updated) |
| `RL_WNDW_CT` | Per-window occupancy counter | Upsert (INSERT or UPDATE +1) |
| `RL_SKIP_PTR` | Per-requestedTime skip pointer | Monotonic UPDATE (never decreases) |

### DB Calls Per Request

| Scenario | Calls | Operations |
|----------|-------|------------|
| Happy path (Phase A, 1st chunk) | 3 | skip pointer read + occupancy read + claim |
| Phase A, 3rd chunk | 5 | skip pointer + 3 occupancy reads + claim |
| Phase B | +2 | fresh occupancy read + claim |
| Phase C (1st extension) | +2 | fresh occupancy read + claim |
| Idempotent duplicate | 3 | skip pointer + occupancy + claim (catches UNIQUE, re-reads) |

---

## Configuration

### Production Config (30 TPS target, 30-second windows)

```yaml
rate-limiter:
  window-size-seconds: 30
  v5:
    soft-max-per-window: 870        # 30 TPS * 30s * 0.97 headroom
    hard-max-per-window: 990        # 30 TPS * 30s * 1.10 burst
    default-max-duration-hours: 8   # Default: slots can go up to 8h out
    phase-a-chunk-seconds: 900      # 15-min chunks (30 windows each)
    extension-windows: 40           # 20-min extension chunks
    max-extensions-beyond: 5        # Up to 5 extensions beyond maxDuration
    max-hard-cap-retries: 3         # Retries on hard-cap rollback
```

### Capacity Math

```
Window size:       30 seconds
softMax:           870 slots/window
Effective TPS:     870 / 30 = 29 TPS (sustained)

hardMax:           990 slots/window
Burst TPS:         990 / 30 = 33 TPS (temporary overflow)

Default maxDuration: 8 hours
Total capacity:      8h * 120 windows/hr * 870 slots = 835,200 events
```

### Recommended Configs by Traffic Pattern

#### Near-Term Sustained (100-400 TPS inbound, 30 TPS downstream)

Requests arrive at 100-400 TPS with `requestedTime = now + 1 minute`. Each request needs a slot within a few hours.

```yaml
v5:
  soft-max-per-window: 870
  hard-max-per-window: 990
  default-max-duration-hours: 8
  phase-a-chunk-seconds: 900      # 15 min — tight proximity
```

**How it behaves**: At 400 TPS inbound / 30 TPS outbound, each second produces ~13 "excess" requests that spill forward. The skip pointer advances ~13 windows/second. An 8-hour maxDuration holds 835K events — sufficient for sustained bursts up to ~30 minutes (400 TPS * 1800s = 720K).

#### Long-Horizon Batch (500K requests at 100 TPS, requestedTime days out)

A batch of 500K payment requests with `requestedTime = now + 7 days`. All slots must be assigned, spread across many hours.

```yaml
v5:
  soft-max-per-window: 870
  hard-max-per-window: 990
  default-max-duration-hours: 24    # Allow wider spread
  phase-a-chunk-seconds: 1800       # 30-min chunks (less DB round-trips)
  max-extensions-beyond: 10         # More room to extend
```

**How it behaves**: 500K events / 870 per window = 575 windows needed = ~4.8 hours of wall-clock time. With 24-hour maxDuration, Phase A alone handles it. The larger chunk size (30 min) reduces DB round-trips since proximity to the exact `requestedTime` matters less for batch jobs.

---

## API

### Request

```
POST /api/v2/slots
```

```json
{
  "eventId": "pay-123",
  "requestedTime": "2025-06-01T14:00:00Z",
  "maxDuration": "PT4H"
}
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `eventId` | Yes | — | Unique idempotency key |
| `requestedTime` | Yes | — | Desired execution time (ISO-8601) |
| `maxDuration` | No | `PT8H` | How far from requestedTime slots can go (ISO-8601 duration) |

### Response

```json
{
  "eventId": "pay-123",
  "scheduledTime": "2025-06-01T14:02:17.483Z",
  "delayMs": 137483,
  "allocationStatus": "NORMAL"
}
```

| `allocationStatus` | Meaning | Caller Action |
|--------------------|---------|---------------|
| `NORMAL` | Slot within maxDuration, below softMax | None — optimal placement |
| `SOFT_MAX_EXCEEDED` | Slot within maxDuration, window between softMax and hardMax | Monitor — nearing capacity |
| `MAX_DURATION_EXCEEDED` | Slot placed beyond caller's maxDuration | Alert — may need to adjust processing timeline |

### Error Response (503)

```json
{
  "error": "No available window for event pay-123 within 1160 windows",
  "eventId": "pay-123",
  "windowsSearched": 1160
}
```

---

## Scenarios

All scenarios use the following config for readability:

| Parameter | Value |
|-----------|-------|
| `windowSize` | 60 seconds (1 minute) |
| `softMax` | 5 slots/window |
| `hardMax` | 7 slots/window |
| `maxDuration` | 10 minutes (default) |
| `phaseAChunkSize` | 4 minutes (4 windows) |
| `extensionWindows` | 3 |
| `maxExtensionsBeyond` | 2 |

Window labels:

| Label | WNDW_STRT_TS |
|-------|--------------|
| W+0 | requestedTime + 0 min |
| W+1 | requestedTime + 1 min |
| W+N | requestedTime + N min |

---

### Scenario 1: Empty Table — First Request

**Request**: `assignSlot("evt-1", 14:00:00)`

**State before**: No counter rows, no skip pointer.

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| Skip pointer | `null` → use `requestedTime` | No pointer for 14:00 |
| Phase A, chunk 1 | `[W+0, W+1, W+2, W+3]` | `[14:00, 14:04)` |
| Occupancy read | `{}` (empty) | No counter rows exist |
| Candidates | All 4 windows, weight > 0 | All have capacity = softMax (5) |
| Proximity pick | W+0 (48%), W+1 (31%), W+2 (19%), W+3 (10%) | Closer windows favored |
| Picked | W+0 *(example)* | |
| Jitter | 23456ms | Random in `[0, 60000)` |
| Claim | INSERT slot + upsert counter (count=1) | Counter row created on demand |

**Result**: `AssignedSlot(evt-1, 14:00:23.456, delay=23.456s, NORMAL)`

**State after**:

| RL_WNDW_CT | |
|---|---|
| W+0: SLOT_CT=1 | Created on demand |

| RL_SKIP_PTR | |
|---|---|
| *(no row)* | No chunks were exhausted |

**Key point**: Counter rows are created on demand — no pre-provisioning needed. The skip pointer is not written because the chunk was not exhausted.

---

### Scenario 2: Phase A Chunking — First Chunk Full, Second Has Capacity

**Request**: `assignSlot("evt-20", 14:00:00)`

**State before**:

| Window | SLOT_CT | Status |
|--------|---------|--------|
| W+0 | 5 | Full (= softMax) |
| W+1 | 5 | Full |
| W+2 | 5 | Full |
| W+3 | 5 | Full |
| W+4 | 1 | Available |
| W+5 | 0 | Available |
| W+6 | 2 | Available |

No skip pointer.

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| Skip pointer | `null` → start at 14:00 | |
| Phase A, chunk 1 | `[W+0..W+3]` | Range `[14:00, 14:04)` |
| Occupancy | `{W+0:5, W+1:5, W+2:5, W+3:5}` | |
| Candidates | None | All at softMax |
| Advance skip pointer | `14:04:00` | Chunk 1 exhausted |
| Phase A, chunk 2 | `[W+4..W+7]` | Range `[14:04, 14:08)` |
| Occupancy | `{W+4:1, W+6:2}` | W+5, W+7 absent → count=0 |
| Candidates | W+4(weight=16), W+5(weight=15), W+6(weight=6), W+7(weight=5) | Proximity-weighted |
| Picked | W+4 *(example — highest weight)* | |
| Claim | INSERT + upsert counter (count=2) | |

**Result**: `AssignedSlot(evt-20, 14:04:XX.XXX, NORMAL)`

**State after**:

| RL_SKIP_PTR | |
|---|---|
| REQ_TS=14:00, SKIP_TO=14:04 | Chunk 1 exhausted |

**Key point**: The skip pointer now points to 14:04. The **next request** for `requestedTime=14:00` will skip chunk 1 entirely and start at chunk 2 — even if it arrives on a different pod.

---

### Scenario 3: Phase B — Overflow Within maxDuration

**Request**: `assignSlot("evt-50", 14:00:00, maxDuration=10min)`

**State before**: All windows in maxDuration at softMax, but below hardMax.

| Window | SLOT_CT | Status |
|--------|---------|--------|
| W+0 | 5 | At softMax |
| W+1 | 5 | At softMax |
| ... | 5 | At softMax |
| W+9 | 5 | At softMax |

Skip pointer at `14:10` (past maxDurationEnd).

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| Skip pointer | `14:10` | Past maxDurationEnd (14:10) |
| Phase A start | `14:10` | `max(skipTo, requestedTime)` |
| Phase A | No chunks to scan | `phaseAStart >= maxDurationEnd` |
| Phase B | Fresh read `[14:00, 14:10)` | Ignores skip pointer |
| Occupancy | `{W+0:5, ..., W+9:5}` | All at softMax (5), below hardMax (7) |
| Candidates | All 10 windows | capacityWeight = 7 - 5 = 2 per window |
| Picked | W+1 *(example)* | Proximity-weighted among overflow candidates |
| Claim | INSERT + upsert counter (count=6) | 6 <= hardMax (7) → success |

**Result**: `AssignedSlot(evt-50, 14:01:XX.XXX, SOFT_MAX_EXCEEDED)`

**Key point**: Phase B scans from `requestedTime`, not from the skip pointer. This ensures windows between softMax and hardMax (which Phase A skipped) are found. The `SOFT_MAX_EXCEEDED` status tells the caller that capacity is tight.

---

### Scenario 4: Phase C — Extension Beyond maxDuration

**Request**: `assignSlot("evt-80", 14:00:00, maxDuration=10min)`

**State before**: All windows in maxDuration at hardMax.

| Window | SLOT_CT | Status |
|--------|---------|--------|
| W+0 to W+9 | 7 | At hardMax |

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| Phase A | Exhausted | All chunks at softMax or higher |
| Phase B | Exhausted | All windows at hardMax |
| Phase C, ext 1 | `[W+10, W+11, W+12]` | 3 extension windows beyond 14:10 |
| Occupancy | `{}` (empty) | Fresh read — no counter rows here |
| Picked | W+10 *(example)* | Closest in extension range |
| Claim | INSERT + upsert counter (count=1) | |

**Result**: `AssignedSlot(evt-80, 14:10:XX.XXX, MAX_DURATION_EXCEEDED)`

**Key point**: The `MAX_DURATION_EXCEEDED` status signals to the caller that the slot was placed beyond their stated tolerance. The caller can decide whether to accept it, retry with a larger `maxDuration`, or take alternative action.

---

### Scenario 5: Per-Request maxDuration Changes Phase Behavior

Two requests for the same `requestedTime` with different `maxDuration` values:

**State before**: Windows W+0 to W+3 at softMax (5). W+4 onwards empty.

**Request A**: `assignSlot("evt-short", 14:00:00, maxDuration=4min)`

| Step | Result |
|------|--------|
| maxDurationEnd | 14:04:00 |
| Phase A | Scans `[W+0..W+3]` — all at softMax → exhausted |
| Phase B | Fresh read `[W+0..W+3]` — room up to hardMax (7) → picks W+0 |
| Status | **SOFT_MAX_EXCEEDED** |

**Request B**: `assignSlot("evt-long", 14:00:00, maxDuration=8min)`

| Step | Result |
|------|--------|
| maxDurationEnd | 14:08:00 |
| Phase A | Scans chunk 1 `[W+0..W+3]` — exhausted. Chunk 2 `[W+4..W+7]` — empty → picks W+4 |
| Status | **NORMAL** |

**Key point**: The same window state produces different outcomes based on `maxDuration`. Request A with a tight maxDuration falls into Phase B; request B with a wider maxDuration finds empty windows in Phase A.

---

### Scenario 6: Shared Capacity Across requestedTimes

**State before**: Requests for `requestedTime=14:00` have filled W+2 (`14:02:00`) to softMax.

| Window | SLOT_CT | Who filled it |
|--------|---------|---------------|
| 14:02:00 | 5 | Requests with requestedTime=14:00 |

**Request**: `assignSlot("evt-B1", 14:02:00)` — different requestedTime, same window.

**Walkthrough**:

| Step | Value | Reasoning |
|------|-------|-----------|
| Phase A, chunk 1 | `[14:02, 14:03, 14:04, 14:05]` | |
| Occupancy | `{14:02:00: 5}` | **Global** counter — sees all slots regardless of requestedTime |
| Candidates | `[14:03, 14:04, 14:05]` | 14:02 filtered out (at softMax) |
| Picked | 14:03 *(example)* | |

**Result**: Slot lands in 14:03, avoiding the full window.

**Key point**: The counter table (`RL_WNDW_CT`) is keyed by window start time only — not by requestedTime. This prevents any window from being overloaded regardless of which requestedTime contributed the slots. The trade-off: high-volume requestedTimes can "crowd out" windows for other requestedTimes that share the same range.

---

### Scenario 7: Concurrent Duplicate — Idempotency via UNIQUE Constraint

Two threads simultaneously call `assignSlot("evt-99", 14:00:00)`.

```
Thread A                              Thread B
────────                              ────────
Read skip pointer → null              Read skip pointer → null
Read occupancy → {}                   Read occupancy → {}
Pick W+1, jitter=23456ms              Pick W+2, jitter=45678ms
BEGIN transaction                     BEGIN transaction
  INSERT slot (evt-99, W+1) → OK        INSERT slot (evt-99, W+2) → UNIQUE violation!
  Upsert counter (W+1) → count=1        Catch duplicate key
  COMMIT                                 Re-read: queryAssignedSlot("evt-99")
                                         → returns Thread A's slot
                                         COMMIT (no counter increment)
```

**Result**: Both threads return `AssignedSlot(evt-99, 14:01:23.456, ...)`. One row in DB. Counter incremented exactly once.

---

### Scenario 8: Hard Cap Rollback and Retry

Multiple concurrent threads target the same window, pushing it past hardMax.

**State before**: W+0 has SLOT_CT=6 (one below hardMax=7).

```
Thread A                              Thread B
────────                              ────────
Read occupancy → W+0: 6              Read occupancy → W+0: 6
Both see room for 1 more             Both see room for 1 more
Pick W+0                              Pick W+0
BEGIN                                 BEGIN
  INSERT slot-A                         INSERT slot-B
  Upsert counter → returns 7             Upsert counter → returns 8
  7 <= hardMax → COMMIT                   8 > hardMax → ROLLBACK!
                                        Re-read occupancy → W+0: 7
                                        Pick W+1 (next best)
                                        INSERT slot-B, upsert counter → 1
                                        COMMIT
```

**Result**: W+0 has exactly 7 slots (hardMax). Thread B's slot landed in W+1. The hard cap is enforced atomically — no window ever exceeds hardMax.

---

### Scenario 9: Skip Pointer Coordination Across Pods

Three pods process requests for the same `requestedTime=14:00` concurrently. Chunk size = 4 windows.

```
Timeline of skip pointer state (RL_SKIP_PTR for REQ_TS=14:00):

  Pod 1: Exhausts chunk [14:00, 14:04) → UPDATE SKIP_TO = 14:04  ✓ (was null)
  Pod 2: Reads SKIP_TO = 14:04 → starts at chunk [14:04, 14:08) → finds slot → done
  Pod 3: Reads SKIP_TO = 14:04 → starts at chunk [14:04, 14:08) → exhausts it
         → UPDATE SKIP_TO = 14:08 (14:08 > 14:04 ✓)
  Pod 1: New request → reads SKIP_TO = 14:08 → starts at chunk [14:08, 14:12)
```

**Key point**: No pod re-scans already-exhausted chunks. The skip pointer is a lightweight coordination primitive — one PK row per requestedTime, updated only when chunks are exhausted.

---

### Scenario 10: Full Exhaustion — All Phases Fail

**Request**: `assignSlot("evt-fail", 14:00:00, maxDuration=10min)`

**State**: All windows in maxDuration at hardMax, all extension windows also at hardMax.

| Range | Windows | Status |
|-------|---------|--------|
| maxDuration [W+0..W+9] | 10 | All at hardMax (7) |
| Extension 1 [W+10..W+12] | 3 | All at hardMax (7) |
| Extension 2 [W+13..W+15] | 3 | All at hardMax (7) |

**Walkthrough**:

| Phase | Result |
|-------|--------|
| Phase A | All chunks exhausted at softMax |
| Phase B | All windows at hardMax |
| Phase C, ext 1 | All windows at softMax+ |
| Phase C, ext 2 | All windows at softMax+ |

**Result**: `SlotAssignmentException(eventId=evt-fail, windowsSearched=16, "No available window...")`

HTTP response: **503 Service Unavailable**

**Key point**: This is a genuine capacity exhaustion — the system searched 16 windows across all three phases and found no room. The caller should retry later or increase `maxDuration`.

---

## Design Trade-offs

| Design Choice | Benefit | Trade-off |
|---------------|---------|-----------|
| **Optimistic inserts (no row locks)** | No lock contention, no deadlocks, scales linearly with pods | Advisory occupancy read can be stale — hard cap needed as safety net |
| **Advisory counter + hard cap** | Fast reads (no lock), atomic enforcement via RETURNING INTO | Two-tier capacity model is more complex than a single limit |
| **DB-backed skip pointer** | Multi-pod coordination without Redis/external cache | Extra DB call per request (PK lookup, ~0.1ms) |
| **Chunked Phase A** | Tight proximity weighting, small occupancy reads | More DB round-trips if many chunks are exhausted |
| **Three-phase allocation** | Graceful degradation with caller visibility | More complex algorithm; caller must handle three status values |
| **No pre-provisioning** | Zero setup, zero background jobs, zero wasted storage | First slot in a window pays the counter INSERT cost (~0.5ms) |
| **Proximity-weighted random** | Balances closeness and load spreading | Non-deterministic — harder to predict exact fill order |
| **Per-request maxDuration** | Flexible per-caller SLAs | Different maxDurations for same requestedTime can cause fragmented fill patterns |
| **Global window capacity** | Prevents overload regardless of requestedTime source | High-volume requestedTimes can crowd out smaller ones sharing the same windows |

---

## Comparison with V3

| Aspect | V3 (Pessimistic) | V5 (Optimistic) |
|--------|-------------------|-----------------|
| Locking | `FOR UPDATE SKIP LOCKED` | None |
| Pre-provisioning | Required (60-day batch cron) | Not needed |
| Counter accuracy | Exact (locked increment) | Advisory read + atomic upsert |
| Capacity model | Single `maxPerWindow` | Soft max + hard max |
| Window selection | Sequential (first available) | Proximity-weighted random |
| Multi-pod coordination | Row locks provide implicit coordination | DB skip pointer |
| Allocation control | Fixed search depth | Per-request `maxDuration` + 3-phase |
| Best for | Strict ordering, exact capacity | High throughput, multi-pod, flexible SLAs |

---

## Key Files

| File | Role |
|------|------|
| `slot/SlotAssignmentServiceV5.kt` | Core three-phase algorithm |
| `repo/WindowSlotCounterRepository.kt` | Occupancy reads, `upsertCounterReturningCount` |
| `repo/SkipPointerRepository.kt` | DB-backed skip pointer (monotonic) |
| `repo/EventSlotRepository.kt` | Slot insertion, idempotency |
| `db/Tables.kt` | `WindowCounterTable`, `SkipPointerTable`, `RateLimitEventSlotTable` |
| `api/SlotAssignmentV2Resource.kt` | REST endpoint with `maxDuration` and `AllocationStatus` |
| `slot/AllocationStatus.kt` | `NORMAL`, `SOFT_MAX_EXCEEDED`, `MAX_DURATION_EXCEEDED` enum |
