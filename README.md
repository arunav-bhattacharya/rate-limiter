# Payments Rate Limiter

Oracle-based rate limiter for high-throughput event scheduling. Exposes a REST API that assigns
rate-limited time slots to events, enforcing a configurable maximum events per time window.

## Quick Start

### Prerequisites

- **JDK**: 21
- **Docker & Docker Compose**: For local Oracle 19c

### 1. Start Oracle and create the schema user

```bash
./scripts/setup-oracle.sh
```

This script:
- Starts Oracle 19c via Docker Compose (`codeassertion/oracledb-arm64-standalone`)
- Waits for the database to be ready (2-4 minutes on first run)
- Creates the `rate_limiter` user in the `ORCLPDB1` pluggable database
- Grants necessary permissions

### 2. Start the application

```bash
./gradlew quarkusDev
```

Flyway will automatically create the tables on startup.

### 3. Seed the default rate limit config

```bash
curl -X POST http://localhost:8080/admin/rate-limit/config \
  -H 'Content-Type: application/json' \
  -d '{
    "configName": "default",
    "maxPerWindow": 100,
    "windowSize": "PT4S"
  }'
```

### 4. Assign a slot

```bash
curl -X POST http://localhost:8080/api/v1/slots \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "pay-123",
    "configName": "default",
    "requestedTime": "2025-06-01T12:00:00Z"
  }'
```

Response:
```json
{
  "eventId": "pay-123",
  "scheduledTime": "2025-06-01T12:00:02.371Z",
  "delayMs": 2371
}
```

## System Overview

```
                     CALLER (up to 1M events/day)
                                |
                                v
                +-------------------------------+
                |    POST /api/v1/slots          |
                |    SlotAssignmentResource      |
                |                               |
                |  Request:                     |
                |    eventId, configName,        |
                |    requestedTime              |
                +-------------------------------+
                                |
                +-------------------------------+
                |   SlotAssignmentService        |
                |                               |
                |  1. Load config (cached)      |
                |  2. Single PL/SQL round trip:  |
                |     idempotency + window walk  |
                |     + lock + insert + jitter   |
                +-------------------------------+
                                |
           +--------------------+--------------------+
           |                    |                    |
    +-----------+        +-----------+        +-----------+
    | Window 0  |        | Window 1  |        | Window N  |
    | t+0s..t+4s|        | t+4s..t+8s|        | ...       |
    | max 100   |        | max 100   |        | max 100   |
    +-----------+        +-----------+        +-----------+
                                |
                                v
                +-------------------------------+
                |  Response:                    |
                |    eventId, scheduledTime,     |
                |    delayMs                    |
                +-------------------------------+
```

## REST API

### Slot Assignment

**POST** `/api/v1/slots`

Assigns a rate-limited time slot for the given event. Idempotent: calling with the same
`eventId` returns the same slot.

**Request:**
```json
{
  "eventId": "pay-123",
  "configName": "default",
  "requestedTime": "2025-06-01T12:00:00Z"
}
```

**Response (200 OK):**
```json
{
  "eventId": "pay-123",
  "scheduledTime": "2025-06-01T12:00:02.371Z",
  "delayMs": 2371
}
```

| Field | Description |
|---|---|
| `eventId` | Echo of the input event ID |
| `scheduledTime` | Actual assigned execution time (ISO-8601) |
| `delayMs` | Milliseconds between `requestedTime` and `scheduledTime` |

**Error Responses:**
- `404` — Config not found for the given `configName`
- `503` — All windows within the dynamic lookahead range are full

### Admin: Config Management

**GET** `/admin/rate-limit/config?name=default` — Get active config

**POST** `/admin/rate-limit/config` — Create/update config (deactivates previous)
```json
{
  "configName": "default",
  "maxPerWindow": 100,
  "windowSize": "PT4S"
}
```

**POST** `/admin/rate-limit/cache/flush` — Force-evict in-memory config cache

## How the Rate Limiter Works

### Window Model

Time is divided into fixed-size windows (default: 4 seconds). Each window has a maximum
capacity (default: 100 events). When an event requests execution at time T:

1. **Snap** T to the nearest window boundary: `windowStart = T - (T % windowSize)`
2. **Lock** the window counter row with `SELECT FOR UPDATE NOWAIT`
3. **Check** if `slot_count < max_per_window`
4. **If yes**: insert slot record, increment counter, compute `scheduled_time = windowStart + random(0, windowSize)`
5. **If no (full)** or **contended (NOWAIT bounce)**: advance to the next window and repeat
6. **Return** the `AssignedSlot` with `scheduledTime` and `delay`

### Single Round-Trip PL/SQL Block

The entire slot assignment — idempotency check, window walk loop, lock acquisition,
slot insertion, and counter update — executes as a **single anonymous PL/SQL block**
in one JDBC round trip. This minimizes network latency and lock hold time.

The PL/SQL block contains three local functions:
- `check_existing_slot`: Idempotency pre-check (SELECT by event_id)
- `try_lock_window`: Ensure counter row + SELECT FOR UPDATE NOWAIT
- `claim_slot_in_window`: INSERT slot + UPDATE counter, with ORA-00001 handling

Threads that lose the lock race (`NOWAIT` bounce) skip to the next window inside
the PL/SQL loop — zero additional network hops for window skipping.

### Random Jitter

All offsets within a window use uniformly random jitter via Oracle's `DBMS_RANDOM`:

```
jitter_ms  = TRUNC(DBMS_RANDOM.VALUE(0, windowSizeMs))
scheduled_time = windowStart + jitter_ms/1000 seconds
```

Random jitter is used exclusively because when `max_per_window` is increased dynamically,
new events must not cluster on deterministic grid points left by previously assigned events.
At high volumes (100+ per window), random distribution is practically uniform.

### Config-Agnostic Counters

The `rate_limit_window_counter` table tracks total events assigned to each window,
regardless of which config version was active when each slot was assigned. This means:

- **Increasing capacity**: New config sees existing occupancy. If window has 80 slots
  and new config allows 200, 120 more slots are available.
- **Decreasing capacity**: Window with 80 slots under old max=100, new max=50: window
  is treated as full. Already-scheduled events are immutable.

### Concurrency Control

- `SELECT FOR UPDATE NOWAIT` serializes concurrent writers per window row.
- `NOWAIT` means threads that lose the lock race immediately skip to the next window.
- This prevents thread convoys during bulk ingestion — essential when 10K+ events
  target the same time window.

### Idempotency

Each event is identified by a unique `event_id`. Calling `assignSlot()` twice with the
same `event_id` returns the same `AssignedSlot`. A UNIQUE constraint on `event_id`
prevents duplicate assignments under concurrent access.

### Slot Assignment Sequence Diagram

```mermaid
sequenceDiagram
    participant Caller
    participant SAS as SlotAssignmentService
    participant Cache as ConfigCache
    participant DB as Oracle DB

    Caller->>SAS: assignSlot(eventId, configName, requestedTime)

    Note over SAS: Step 1 — Load config
    SAS->>Cache: loadActiveConfig(configName)
    alt Cache miss
        Cache->>DB: SELECT * FROM rate_limit_config WHERE config_name = ? AND is_active = 1
        DB-->>Cache: config row
    end
    Cache-->>SAS: RateLimitConfig

    Note over SAS: Step 2 — Compute search range
    SAS->>SAS: windowStart = align(requestedTime)<br/>searchLimit = max(frontier, windowStart) + headroom

    Note over SAS: Step 3 — Single PL/SQL round trip
    SAS->>DB: CallableStatement: anonymous PL/SQL block<br/>(8 IN params, 5 OUT params)

    Note over DB: PL/SQL block executes server-side:<br/>1. Idempotency pre-check (SELECT by event_id)<br/>2. Window walk loop:<br/>   - try_lock_window (INSERT counter + SELECT FOR UPDATE NOWAIT)<br/>   - Skip on ORA-00054/ORA-00060 (contended)<br/>   - Skip if slot_count >= max_per_window (full)<br/>   - claim_slot_in_window (INSERT slot + UPDATE counter)<br/>   - DBMS_RANDOM jitter per window<br/>3. Marshal results to OUT params

    DB-->>SAS: ou_status, ou_slot_id, ou_scheduled_time,<br/>ou_window_start, ou_windows_searched

    Note over SAS: Step 4 — Interpret result (pure Kotlin)
    alt status = NEW (slot assigned)
        SAS->>SAS: update furthestAssignedWindow atomically
        SAS-->>Caller: return AssignedSlot
    else status = EXISTING (idempotent hit)
        SAS-->>Caller: return AssignedSlot (same slot)
    else status = EXHAUSTED (no capacity)
        SAS-->>Caller: throw SlotAssignmentException
    end
```

### Slot Assignment Flow Diagram

```mermaid
flowchart TD
    A([assignSlot called]) --> D[Load RateLimitConfig<br/>from cache or DB]
    D --> E{Config found?}
    E -- No --> F([Throw ConfigLoadException])
    E -- Yes --> G["Compute search range<br/>windowStart = align(requestedTime)<br/>searchLimit = max(frontier, windowStart) + headroom"]

    G --> PL["<b>Execute PL/SQL block</b><br/>(single JDBC round trip)"]

    subgraph PLSQL ["PL/SQL Block (server-side)"]
        direction TD
        B{check_existing_slot:<br/>event_id already exists?}
        B -- Yes --> C2[/"ou_status = EXISTING"/]
        B -- No --> H{current_window<br/>> search_limit?}
        H -- Yes --> I2[/"ou_status = EXHAUSTED"/]

        H -- No --> J["try_lock_window:<br/>INSERT counter row (catch DUP_VAL_ON_INDEX)<br/>SELECT slot_count FOR UPDATE NOWAIT"]
        J --> L{ORA-00054 /<br/>ORA-00060?}

        L -- Yes --> M[/"Contended — skip"/]
        M --> N["current_window += window_size"]
        N --> H

        L -- No --> O{slot_count >=<br/>max_per_window?}
        O -- Yes --> P[/"Full — skip"/]
        P --> N

        O -- No --> Q["claim_slot_in_window:<br/>jitter = DBMS_RANDOM.VALUE(0, windowSizeMs)<br/>INSERT slot + UPDATE counter"]
        Q --> S{DUP_VAL_ON_INDEX<br/>on event_id?}

        S -- Yes --> T[/"Idempotency race<br/>ou_status = EXISTING"/]
        S -- No --> U[/"ou_status = NEW"/]
    end

    PL --> PLSQL
    C2 --> RET
    I2 --> RET
    T --> RET
    U --> RET

    RET["Marshal OUT params:<br/>status, slot_id, scheduled_time,<br/>window_start, windows_searched"]

    RET --> INT{Interpret status}
    INT -- NEW --> X["Update furthestAssignedWindow<br/>atomically"]
    X --> Y([Return new AssignedSlot])
    INT -- EXISTING --> C([Return existing AssignedSlot])
    INT -- EXHAUSTED --> I([Throw SlotAssignmentException])

    style A fill:#4a9eff,color:#fff
    style C fill:#2ecc71,color:#fff
    style Y fill:#2ecc71,color:#fff
    style F fill:#e74c3c,color:#fff
    style I fill:#e74c3c,color:#fff
    style M fill:#f39c12,color:#fff
    style P fill:#f39c12,color:#fff
    style T fill:#e67e22,color:#fff
    style PL fill:#3498db,color:#fff
```

## Configuration Reference

All properties are set in `src/main/resources/application.yaml`:

| Property | Description | Default |
|---|---|---|
| `rate-limiter.default-config-name` | Name of the default rate limit config | `default` |
| `rate-limiter.headroom-windows` | How many windows beyond the frontier to search | `100` |
| `quarkus.datasource.db-kind` | Database type | `oracle` |
| `quarkus.datasource.jdbc.url` | Oracle JDBC URL | `jdbc:oracle:thin:@localhost:1521/ORCLPDB1` |
| `quarkus.datasource.username` | Oracle username | `rate_limiter` |
| `quarkus.datasource.password` | Oracle password | `rate_limiter` |
| `quarkus.datasource.jdbc.min-size` | Minimum connection pool size | `5` |
| `quarkus.datasource.jdbc.max-size` | Maximum connection pool size | `30` |
| `quarkus.flyway.migrate-at-start` | Run Flyway migrations at startup | `true` |

## Docker Setup

The project includes a `docker-compose.yml` and setup script for Oracle 19c:

```bash
# Start Oracle and create the rate_limiter user
./scripts/setup-oracle.sh

# Or manually via docker-compose
docker compose up -d oracle
```

The Docker setup uses [`codeassertion/oracledb-arm64-standalone:19.3.0-enterprise`](https://hub.docker.com/r/codeassertion/oracledb-arm64-standalone),
which supports ARM64 (Apple Silicon M1/M2/M3).

**Connection details after setup:**

| Parameter | Value |
|---|---|
| JDBC URL | `jdbc:oracle:thin:@localhost:1521/ORCLPDB1` |
| Username | `rate_limiter` |
| Password | `rate_limiter` |

### Stopping and cleaning up

```bash
# Stop Oracle (data persists in Docker volume)
docker compose down

# Stop and remove data
docker compose down -v
```

## Dynamic Config Update Guide

### Increasing Capacity

To increase from 100 to 200 events per window:

```bash
curl -X POST http://localhost:8080/admin/rate-limit/config \
  -H 'Content-Type: application/json' \
  -d '{
    "configName": "default",
    "maxPerWindow": 200,
    "windowSize": "PT4S"
  }'
```

This inserts a new config row and deactivates the old one. The change takes effect
within 5 seconds (cache TTL) on all nodes.

**What happens**: Windows partially filled under the old config continue filling under
the new limit. Existing scheduled events are never modified.

### Decreasing Capacity

Same API, lower value. Windows already exceeding the new limit are treated as full.
No existing events are cancelled.

**What NOT to change**:
- Do not change `window_size` while events are in-flight. This changes the window
  boundaries and makes existing counter rows meaningless.
- Do not manually edit `rate_limit_window_counter` rows.
- Do not delete `rate_limit_config` rows — deactivate them instead.

### Cache Flush (Urgent Changes)

For immediate propagation across all nodes:

```bash
curl -X POST http://localhost:8080/admin/rate-limit/cache/flush
```

## Observability

### Key Log Messages

- `INFO  SlotAssignmentService - Assigned slot for eventId={} in window={} after searching {} windows`
- `DEBUG SlotAssignmentService - Idempotent hit for eventId={}`
- `ERROR SlotAssignmentService - Could not assign slot for event {} after searching {} windows`
- `INFO  SlotAssignmentService - Initialized furthestAssignedWindow from DB: {}`
- `INFO  RateLimitConfigRepository - Config cache miss for {configName}, loaded from DB`

## Known Limitations

1. **TPS guarantee is statistical, not absolute**: Random jitter within windows means
   instantaneous bursts can theoretically exceed the per-window limit for brief
   sub-second intervals.

2. **Lookahead exhaustion**: If a single burst exceeds `headroom-windows * maxPerWindow`
   events (default: 100 * 100 = 10,000), slot assignment fails for remaining events.
   The dynamic lookahead grows with load, so this is only a concern for sudden massive spikes.

3. **Config propagation delay**: Config changes take up to 5 seconds (cache TTL) to
   propagate to all nodes. Use the cache flush endpoint for immediate propagation.

4. **No business-hours awareness**: The window model advances linearly through time
   with no concept of business hours or blackout periods.

## Operational Runbook

### Windows Filling Up (Lookahead Approaching Limit)

**Symptom**: Logs show `Lookahead exhausted` errors or high lookahead depths.

**Action**:
1. Check current config: `GET /admin/rate-limit/config`
2. If safe, increase `maxPerWindow`: `POST /admin/rate-limit/config`
3. Check logs for `SlotAssignmentException` — if present, events are being rejected.

### Oracle Slow / Unavailable

**Symptom**: Logs show slow assignment times or connection pool exhaustion warnings.

**Action**:
1. Check Oracle AWR/ASH reports for contention.
2. Verify connection pool is not exhausted: check Quarkus Agroal datasource logs.
3. If Oracle is down, the caller should retry with backoff.
