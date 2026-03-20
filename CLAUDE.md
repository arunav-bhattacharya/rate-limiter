# Rate-Limiter Project

## Overview

Kotlin/Quarkus rate-limiter service using Oracle DB. Assigns time slots to events within fixed windows to enforce rate limits. **SlotAssignmentServiceV3** is the approved implementation.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Kotlin 21 |
| Framework | Quarkus 3 + JAX-RS |
| ORM | Jetbrains Exposed 0.61.0 |
| Database | Oracle 19c |
| Build | Gradle (Kotlin DSL) |
| DI | Jakarta CDI |

## Key Domain Concepts

### Windows

A **window** is a fixed-duration time interval used as the fundamental unit of rate limiting.

- **Duration**: Configurable, default 4 seconds (`windowSize` in config)
- **Epoch-aligned**: Window boundaries are deterministic, calculated as:
  ```
  alignedStart = epochSecond - (epochSecond % windowSizeSecs)
  ```
- **Capacity**: Each window has a maximum number of slots (`maxPerWindow`)
- **Example** (4-second windows):
  ```
  Window 0: 2025-06-01T12:00:00Z → 12:00:04Z
  Window 1: 2025-06-01T12:00:04Z → 12:00:08Z
  Window 2: 2025-06-01T12:00:08Z → 12:00:12Z
  ```

**Why epoch-aligned?** Ensures all service instances agree on window boundaries without coordination. Any request at `T` maps deterministically to the same window regardless of which node handles it.

### Slots

A **slot** is an assigned execution time for an event within a window.

- **Granularity**: Millisecond-precise
- **Jitter**: Random offset within window to spread load:
  - First window: `jitter ∈ [elapsedMs, windowSizeMs)` (ensures `scheduledTime >= requestedTime`)
  - Subsequent windows: `jitter ∈ [0, windowSizeMs)` (full window width)
- **Idempotency**: One slot per unique `eventId` via `UNIQUE(EVENT_ID)` constraint
- **Immutability**: Once assigned, slots are never modified or deleted

**Slot assignment result**:
```kotlin
AssignedSlot(
    eventId = "pay-123",
    scheduledTime = Instant.parse("2025-06-01T12:00:02.371Z"),
    delay = Duration.ofMillis(2371)
)
```

### Chunks

A **chunk** is a batch of windows provisioned together to amortize provisioning cost.

- **Size**: Configurable via `max-windows-in-chunk` (default: 100 windows)
- **Duration**: `chunkSize = windowSize * maxWindowsInChunk` (default: 400 seconds)
- **Provisioning**: Batch-insert counter rows via `batchInsertWindows()`
- **Guard**: Existence check on last window prevents thundering herd
- **Idempotency**: Duplicate exceptions caught silently

**Why chunks?** Creating one row per window on-demand would cause N database round-trips. Batch provisioning amortizes this to 1 round-trip per 100 windows.

### Frontier

The **frontier** is the furthest provisioned window boundary for a given `alignedStart`.

- **Storage**: `RL_WNDW_FRONTIER_TRK(REQ_TS, WNDW_END_TS)` table
- **Key**: `REQ_TS` = `alignedStart` (the epoch-aligned first window)
- **Value**: `WNDW_END_TS` = furthest provisioned boundary
- **Read pattern**: `SELECT MAX(WNDW_END_TS) WHERE REQ_TS = alignedStart`
- **Write pattern**: `INSERT (alignedStart, chunkEnd)` — append-only, duplicates caught via composite PK

**Why frontier tracking?** Without it, each request would scan from window 0 to find available capacity. With frontier tracking, requests jump directly to the known provisioned boundary.

### Proportional Capacity

The **first window** has reduced capacity proportional to remaining time.

```
maxFirstWindow = floor(maxPerWindow * remainingMs / windowSizeMs)
```

**Example**: If `maxPerWindow=100`, `windowSize=4s`, and request arrives 3s into the window:
```
remainingMs = 1000ms
maxFirstWindow = floor(100 * 1000 / 4000) = 25 slots
```

**Why?** Prevents overscheduling. If 100 events are scheduled in the last 1 second of a window, the downstream system would see a 4x spike.

### alignedStart vs requestedTime

Two timestamps flow through the system:

| Timestamp | Description | Used For |
|-----------|-------------|----------|
| `requestedTime` | Original request timestamp (arbitrary) | Slot's `REQ_TS`, delay calculation |
| `alignedStart` | Epoch-aligned window boundary | Window counter key, frontier key, first window identification |

**Critical distinction**: The frontier tracker is keyed by `alignedStart`, not `requestedTime`. Multiple requests with different `requestedTime` values that fall within the same window share the same `alignedStart`.

---

## Core Algorithm (V3)

### Phase Overview

| Phase | Transaction | Operations | Hold Time |
|-------|-------------|------------|-----------|
| 0 | Own (~1ms) | Idempotency check | ~1ms |
| Loop (provision) | Own (~10-50ms) | Fetch frontier or provision chunk | ~10-50ms |
| Loop (find+lock) | Own (~3-5ms) | CASE-based find + lock + claim | ~3-5ms |

### Algorithm Flow

```
assignSlot(eventId, configName, requestedTime)
│
├── Load config (cached 5s TTL)
│
├── Phase 0: Idempotency Check (own transaction ~1ms)
│   └── fetchAssignedSlot(eventId) → return if exists
│
├── Unified Loop (0..maxChunksToSearch):
│   ├── Iteration 0: fetchOrProvisionChunk(alignedStart)
│   │   └── Read MAX frontier OR provision 100 windows + insert frontier
│   ├── Iteration 1+: provisionChunk(provisionFrom, windowCount, windowSize)
│   │   └── Batch-provision new chunk + insert frontier
│   │
│   ├── findLockWindowAndClaimSlot(alignedStart, chunkEnd)
│   │   └── CASE-based SQL: proportional capacity for alignedStart,
│   │       full capacity for all others. Cursor-based FOR UPDATE SKIP LOCKED.
│   │       Re-scans from alignedStart each iteration.
│   │
│   └── provisionFrom = chunkEnd (advance frontier)
│
└── Exhaustion: throw SlotAssignmentException
```

### Key Design Patterns

| Pattern | Implementation | Benefit |
|---------|----------------|---------|
| Split transactions | Each phase in separate `transaction {}` | Minimal connection hold time (~3-10ms total) |
| CASE-based find+lock | `SLOT_CT < CASE WHEN WNDW_STRT_TS = ? THEN ? ELSE ? END` | Single query handles proportional + full capacity |
| Cursor control | `fetchSize=1`, `rowPrefetch=1`, FOR UPDATE SKIP LOCKED | Lazy cursor skips locked rows server-side |
| Re-scan from alignedStart | Each iteration searches `[alignedStart, chunkEnd)` | Recovers previously SKIP LOCKED windows |
| Append-only frontier | Composite PK, no UPDATE | No contention on frontier writes |
| Proportional capacity | `floor(max * remainingMs / windowSizeMs)` | Prevent first-window overscheduling |
| Batch provisioning | 100 windows per chunk | Amortized provisioning cost |

---

## SQL Patterns

### CASE-Based Find+Lock

```sql
SELECT WNDW_STRT_TS FROM RL_WNDW_CT
WHERE WNDW_STRT_TS >= :alignedStart AND WNDW_STRT_TS < :chunkEnd
  AND SLOT_CT < CASE WHEN WNDW_STRT_TS = :alignedStart
                     THEN :maxFirstWindow ELSE :maxPerWindow END
ORDER BY WNDW_STRT_TS ASC
FOR UPDATE SKIP LOCKED
```

- **CASE**: Applies proportional capacity to first window, full capacity to others
- **Cursor**: `fetchSize=1`, `rowPrefetch=1` — Oracle scans lazily, skips locked rows server-side
- **SKIP LOCKED**: Concurrent threads lock different rows without blocking

---

## Database Schema

### Tables

| Table | Purpose | Key Pattern |
|-------|---------|-------------|
| `RL_EVENT_WNDW_CONFIG` | Versioned config | `(CONFIG_NAME, ACT_IN=1)` → one active per name |
| `RL_WNDW_CT` | Per-window slot counter | `PK(WNDW_STRT_TS)`, target of FOR UPDATE SKIP LOCKED |
| `RL_EVENT_SLOT_DTL` | Immutable slot assignments | `UNIQUE(EVENT_ID)` for idempotency |
| `RL_WNDW_FRONTIER_TRK` | Append-only frontier | `PK(REQ_TS, WNDW_END_TS)` composite |

### RL_WNDW_CT (Window Counter)

```sql
CREATE TABLE RL_WNDW_CT (
    WNDW_STRT_TS  TIMESTAMP(6) PRIMARY KEY,  -- epoch-aligned window start
    SLOT_CT       NUMBER(10) DEFAULT 0,       -- current occupancy
    CREAT_TS      TIMESTAMP(6) NOT NULL
);
```

- **Config-agnostic**: Counter tracks total events regardless of config version
- **Dynamic capacity**: Capacity changes take effect immediately on existing windows

### RL_EVENT_SLOT_DTL (Slot Detail)

```sql
CREATE TABLE RL_EVENT_SLOT_DTL (
    WNDW_SLOT_ID       VARCHAR2(50) PRIMARY KEY,
    EVENT_ID           VARCHAR2(50) UNIQUE,      -- idempotency key
    REQ_TS             TIMESTAMP(6) NOT NULL,    -- original requestedTime
    RL_WNDW_CONFIG_ID  VARCHAR2(50) NOT NULL,    -- config version at assignment
    WNDW_STRT_TS       TIMESTAMP(6) NOT NULL,    -- assigned window
    COMPUTED_SCHED_TS  TIMESTAMP(6) NOT NULL,    -- scheduledTime (window + jitter)
    CREAT_TS           TIMESTAMP(6) NOT NULL
);
```

- **Immutable**: INSERT-only, never updated or deleted
- **Audit trail**: Tracks which config version assigned each slot

### RL_WNDW_FRONTIER_TRK (Frontier Tracker)

```sql
CREATE TABLE RL_WNDW_FRONTIER_TRK (
    REQ_TS       TIMESTAMP(6) NOT NULL,  -- alignedStart
    WNDW_END_TS  TIMESTAMP(6) NOT NULL,  -- furthest provisioned boundary
    CREAT_TS     TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (REQ_TS, WNDW_END_TS)    -- composite PK allows multiple rows
);
```

- **Append-only**: No UPDATE statements, concurrent threads deduplicate via PK constraint
- **Read**: `SELECT MAX(WNDW_END_TS) WHERE REQ_TS = ?`

---

## Key Files

| File | Role |
|------|------|
| `slot/SlotAssignmentServiceV3.kt` | Core algorithm orchestration |
| `repo/WindowSlotCounterRepository.kt` | Window locking, nested subquery patterns |
| `repo/EventSlotRepository.kt` | Slot insertion, idempotency checks |
| `repo/WindowEndTrackerRepository.kt` | Frontier tracking (append-only) |
| `repo/RateLimitConfigRepository.kt` | Config loading with 5s cache |
| `db/Tables.kt` | Exposed ORM table definitions |
| `api/SlotAssignmentResource.kt` | REST endpoint for slot assignment |
| `admin/RateLimitAdminResource.kt` | Config management endpoints |

---

## Configuration

### application.yaml

```yaml
rate-limiter:
  max-windows-in-chunk: 100    # Windows per provisioning batch
  max-chunks-to-search: 2      # Extension iterations after initial range
```

### Scaling Parameters

| Parameter | Default | Search Depth |
|-----------|---------|--------------|
| max-windows-in-chunk | 100 | Windows per chunk |
| max-chunks-to-search | 2 | Total iterations |
| **Total** | - | 200 windows = 800s = 20,000 events |

### Connection Pool (Agroal)

```yaml
jdbc:
  min-size: 100              # Pre-warmed connections
  max-size: 150              # Burst capacity
  acquisition-timeout: 5S
  leak-detection-interval: 10S
  transactions: disabled     # No JTA (Exposed manages transactions)
```

---

## REST API

### Slot Assignment

**POST** `/api/v1/slots`

#### Request
```json
{
  "eventId": "pay-123",
  "configName": "default",
  "requestedTime": "2025-06-01T12:00:00Z"
}
```

#### Response (200 OK)
```json
{
  "eventId": "pay-123",
  "scheduledTime": "2025-06-01T12:00:02.371Z",
  "delayMs": 2371
}
```

| Status | Condition |
|--------|-----------|
| 200 | Slot assigned (or existing returned) |
| 404 | Config not found |
| 503 | All windows exhausted |

### Config Management

- **GET** `/admin/rate-limit/config?name=default` — Get active config
- **POST** `/admin/rate-limit/config` — Create/update config
- **POST** `/admin/rate-limit/cache/flush` — Force cache eviction

---

## Error Handling

| Exception | Cause | HTTP | Recovery |
|-----------|-------|------|----------|
| `SlotAssignmentException` | All windows exhausted | 503 | Retry (extends frontier) |
| `ConfigLoadException` | Config not found | 404 | Create config first |
| `ExposedSQLException` (duplicate key) | Concurrent insert | N/A | Re-read existing slot |

---

## Concurrency Guarantees

1. **Row-level locking**: `FOR UPDATE SKIP LOCKED` prevents blocking
2. **Idempotency**: `UNIQUE(EVENT_ID)` + Phase 0 pre-check + duplicate-key recovery
3. **No frontier contention**: Append-only with composite PK
4. **Atomic counter**: INCREMENT in same transaction as slot insert

---

## Testing

- **Framework**: JUnit 5 + Quarkus Test
- **Database**: TestContainers with Oracle XE
- **Key test classes**:
  - `SlotAssignmentServiceV3Test` — Core functional tests
  - `SlotAssignmentServiceV3SqlTest` — SQL-specific tests

**Note**: ARM Mac runs amd64 emulation (slow), may cause timeout flakiness.

---

## Common Gotchas

1. **alignedStart vs requestedTime**: Frontier tracker is keyed by `alignedStart`, not raw `requestedTime`
2. **Exposed DSL limitations**: Doesn't support `FOR UPDATE SKIP LOCKED` — use raw SQL via `exec()`
3. **Connection pool**: Set `transactions: disabled` in Agroal to avoid double-wrapping with Exposed
