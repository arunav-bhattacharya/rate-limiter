# Payments Rate Limiter — Implementation Plan

## Context

A bulk payments processing platform needs an Oracle-based rate limiter to enforce ≤50 TPS when scheduling Temporal workflows. The system receives up to 1M payment events/day in bulk feeds, all potentially targeting the same execution timestamp. Ingestion rate is 100–150 TPS. A bulk load of 500K events at 100 TPS all targeting the same time must be handled. The rate limiter divides time into fixed windows, assigns random-jittered slots within each window, and uses `SELECT FOR UPDATE NOWAIT` for lock-free concurrency control. This is a greenfield project.

## Version Constraints

- **Quarkus**: 3.29.2
- **Temporal SDK**: 1.31.0
- **Kotlin Exposed**: 0.61.0
- **Gradle**: 8.14.1
- **Kotlin**: 2.1.x (compatible with Quarkus 3.29.2)
- **JDK**: 21

## Key Design Decisions

1. **No UNIQUE constraint on `(window_start, scheduled_time)`** — millisecond collisions expected at 100 events/4s window; TPS enforced by slot_count, not time uniqueness
2. **UNIQUE constraint on `event_id`** — idempotency enforcement; handle ORA-00001 with retry-read
3. **`SELECT FOR UPDATE NOWAIT` + skip-to-next-window** — prevents thread convoys during bulk ingestion
4. **Config-agnostic window counters** — counter tracks total across all config versions
5. **5-second in-memory config cache** via `ConcurrentHashMap`
6. **Random-only jitter** — uniform distribution within each window
7. **INSERT + catch ORA-00001** for window counter row creation (not MERGE/insertIgnore) — INSERT is lighter than MERGE; conflict only on first event per window; Oracle detects duplicates at index level cheaply
8. **Dynamic lookahead** — no fixed MAX_LOOKAHEAD; computed as `furthestAssignedWindow + headroom` (see below)

## Dynamic Lookahead

Instead of a fixed `MAX_LOOKAHEAD`, the search limit is dynamically computed:

```
searchLimit = max(furthestAssignedWindow, startingWindow) + headroom
```

- **`furthestAssignedWindow`**: In-memory `AtomicReference<Instant>` tracking the furthest window that has been assigned a slot. Updated atomically on every successful assignment.
- **`headroom`**: Fixed configurable value (default: 100 windows). How far past the frontier we search for empty windows.
- **On startup**: Initialized from DB via `SELECT MAX(window_start) FROM rate_limit_window_counter WHERE slot_count > 0`.

**Behavior**:
- First event: no windows exist → limit = `startingWindow + 100 windows`. Event gets window 0.
- After 5,000 windows filled: limit = `window 4,999 + 100 = window 5,099`. System grew organically.
- No static ceiling that needs manual tuning. The frontier just advances as load increases.
- Headroom prevents infinite search on sparse data — if 100 consecutive empty windows are found past the frontier, we stop.

**Multi-node consistency**: Each node tracks its own `furthestAssignedWindow`. Node A may be at window 3,000 while Node B is at 3,500. Node A's limit is 3,100, Node B's is 3,600. This is safe — Node A simply has a tighter search range and may stop earlier. On the next DB query (startup or a future enhancement), nodes re-sync. The headroom provides buffer for inter-node skew.

## Capacity Planning for 500K Bulk Load

**Scenario**: 500K events at 100 TPS, all requesting the same `scheduledTime`.

| Parameter | Value | Rationale |
|---|---|---|
| Ingestion duration | 5,000s (~83 min) | 500K / 100 TPS |
| Windows needed | 5,000 | 500K / 100 per window |
| Time spread | ~5.5 hours | 5,000 × 4s |
| Dynamic lookahead | Grows from 100 to 5,100 | Frontier advances with each fill |
| DB ops/sec (hot path) | ~300 | 100 TPS × 3 ops (SELECT FOR UPDATE + UPDATE + INSERT) |
| Lock hold time per tx | ~3-5ms | UPDATE counter + INSERT slot + commit |
| Effective lock utilization | ~50% at 100 TPS | 100 × 5ms = 500ms/sec of lock hold |

**Self-distributing behavior**: At 100-150 TPS with multiple concurrent threads, NOWAIT bounces cause threads to spread across windows automatically. The system is NOT bottlenecked on a single window's lock — different threads work on different windows in parallel.

## Files to Create (in implementation order)

### Build & Config (Phase 1)

| # | File | Responsibility |
|---|---|---|
| 1 | `gradle.properties` | Version properties for Quarkus 3.29.2, Kotlin, Gradle plugin |
| 2 | `settings.gradle.kts` | Plugin management, root project name |
| 3 | `build.gradle.kts` | Dependencies, allOpen config, compiler settings |
| 4 | `gradle/wrapper/gradle-wrapper.properties` | Gradle 8.14.1 wrapper config |

### Schema & Tables (Phase 2)

| # | File | Responsibility |
|---|---|---|
| 5 | `src/main/resources/db/migration/V1__rate_limiter_schema.sql` | Flyway DDL: 3 tables, indexes, constraints |
| 6 | `src/main/kotlin/com/payments/ratelimiter/db/Tables.kt` | Exposed Table objects mirroring DDL exactly |
| 7 | `src/main/kotlin/com/payments/ratelimiter/db/ExposedDatabaseInitializer.kt` | CDI bean connecting Exposed to Quarkus Agroal DataSource |

### Domain Models (Phase 3)

| # | File | Responsibility |
|---|---|---|
| 8 | `src/main/kotlin/com/payments/ratelimiter/config/RateLimitConfig.kt` | Immutable domain model data class |
| 9 | `src/main/kotlin/com/payments/ratelimiter/slot/AssignedSlot.kt` | Result type + `sealed class WindowResult` + `SlotAssignmentException` |

### Data Access & Metrics (Phase 4)

| # | File | Responsibility |
|---|---|---|
| 10 | `src/main/kotlin/com/payments/ratelimiter/config/RateLimitConfigRepository.kt` | Config CRUD + 5s ConcurrentHashMap cache |
| 11 | `src/main/kotlin/com/payments/ratelimiter/metrics/RateLimiterMetrics.kt` | Micrometer counters, timers, histograms |

### Core Algorithm (Phase 5)

| # | File | Responsibility |
|---|---|---|
| 12 | `src/main/kotlin/com/payments/ratelimiter/slot/SlotAssignmentService.kt` | Window walk, dynamic lookahead, FOR UPDATE NOWAIT, jitter, idempotency |

### Integration Layer (Phase 6)

| # | File | Responsibility |
|---|---|---|
| 13 | `src/main/kotlin/com/payments/ratelimiter/scheduler/RateLimitedScheduler.kt` | Temporal integration: slot assignment → startDelay → workflow start |
| 14 | `src/main/kotlin/com/payments/ratelimiter/admin/RateLimitAdminResource.kt` | JAX-RS `@Path("/admin/rate-limit")` for config CRUD + cache flush |
| 15 | `src/main/resources/application.properties` | All config keys with defaults |

### Tests (Phase 7)

| # | File | Responsibility |
|---|---|---|
| 16 | `src/test/kotlin/com/payments/ratelimiter/OracleTestResource.kt` | Testcontainers Oracle XE lifecycle manager |
| 17 | `src/test/kotlin/com/payments/ratelimiter/RateLimitConfigRepositoryTest.kt` | Config insert/load/cache/deactivate tests |
| 18 | `src/test/kotlin/com/payments/ratelimiter/SlotAssignmentServiceTest.kt` | Idempotency, window walk, jitter bounds, dynamic lookahead, metrics |
| 19 | `src/test/kotlin/com/payments/ratelimiter/ConcurrencyTest.kt` | 100 concurrent threads, no collisions, no deadlocks, counter consistency |

## Dependency Graph

```
Tables.kt ──────────────────────────────────────────┐
    ↑                                                │
    ├── RateLimitConfig.kt (domain model, no deps)   │
    ├── AssignedSlot.kt (result types, no deps)       │
    │                                                │
    ├── RateLimitConfigRepository.kt                 │
    │       depends on: Tables.kt, RateLimitConfig   │
    │                                                │
    ├── RateLimiterMetrics.kt                        │
    │       depends on: MeterRegistry                │
    │                                                │
    ├── SlotAssignmentService.kt                     │
    │       depends on: Tables.kt, RateLimitConfig,  │
    │         AssignedSlot, ConfigRepository, Metrics │
    │                                                │
    ├── RateLimitedScheduler.kt                      │
    │       depends on: SlotAssignmentService,        │
    │         AssignedSlot, Temporal SDK              │
    │                                                │
    └── RateLimitAdminResource.kt                    │
            depends on: RateLimitConfigRepository     │
```

## SlotAssignmentService — Core Algorithm

```
assignSlot(eventId, configName, requestedTime):
  1. findExistingSlot(eventId) → if found, return (idempotent)
  2. loadActiveConfig(configName) → from cache or DB
  3. windowStart = alignToWindowBoundary(requestedTime, windowSizeSecs)
  4. searchLimit = max(furthestAssignedWindow, windowStart) + (headroom * windowSizeSecs)
  5. while windowStart <= searchLimit:
     a. transaction {
        - INSERT counter row (plain INSERT, not MERGE)
          → catch ORA-00001: row already exists, proceed
        - SELECT slot_count FROM counter WHERE window_start=? FOR UPDATE NOWAIT
          → catch ORA-00054/ORA-00060 → WindowResult.Contended → next window
        - if slot_count >= maxPerWindow → WindowResult.Full → next window
        - scheduledTime = windowStart + random(0, windowSizeMs)
        - INSERT into rate_limit_event_slot
          → catch ORA-00001 on event_id → idempotency race → re-read (counter never touched)
        - UPDATE slot_count = slot_count + 1  (only after INSERT succeeds)
        - return WindowResult.Assigned
     }
     b. On success: furthestAssignedWindow.updateAndGet { max(it, windowStart) }
     c. windowStart += windowSizeSecs
  6. throw SlotAssignmentException (search limit exhausted)

@PostConstruct init:
  furthestAssignedWindow = SELECT MAX(window_start)
                           FROM rate_limit_window_counter
                           WHERE slot_count > 0
```

**Key changes from original design**:
- Counter row creation uses plain `INSERT` + catch ORA-00001 instead of `MERGE` (insertIgnore)
- Loop bound is dynamic (`searchLimit`) not fixed (`MAX_LOOKAHEAD`)
- `furthestAssignedWindow` is an in-memory `AtomicReference<Instant>`, updated on every successful assignment
- Initialized from DB at startup to survive restarts

## Testing Strategy

- **Unit-like tests** (with Testcontainers Oracle XE): SlotAssignmentService is deeply coupled to Oracle SQL semantics, so tests use real Oracle via Testcontainers rather than mocks
- **Config repository tests**: Insert/load/cache TTL/deactivate
- **Dynamic lookahead tests**: Verify frontier advances, headroom bounds search, startup initialization from DB
- **Concurrency test**: 100 threads, same target time, verify:
  - No window exceeds `max_per_window`
  - Total slots == total events submitted
  - Counter values match actual slot counts per window
  - No deadlocks or unhandled exceptions
  - Idempotency: same `event_id` 10× returns identical `AssignedSlot`

## Verification

1. **Build**: `./gradlew build` — compiles, runs all tests
2. **Start Oracle XE**: `docker run -d --name oracle-xe -p 1521:1521 -e ORACLE_PASSWORD=rate_limiter -e APP_USER=rate_limiter -e APP_USER_PASSWORD=rate_limiter gvenzl/oracle-xe:21-slim`
3. **Start Temporal**: `temporal server start-dev`
4. **Run app**: `./gradlew quarkusDev`
5. **Seed config**: `curl -X POST localhost:8080/admin/rate-limit/config -H 'Content-Type: application/json' -d '{"configName":"default","maxPerWindow":100,"windowSizeSecs":4}'`
6. **Check metrics**: `curl localhost:8080/q/metrics | grep rate_limiter`
7. **Run tests**: `./gradlew test` (requires Docker for Testcontainers)
