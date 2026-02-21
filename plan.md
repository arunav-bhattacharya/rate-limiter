# Payments Rate Limiter — Implementation Plan

## Context

A bulk payments processing platform needs an Oracle-based rate limiter to enforce controlled TPS when scheduling events. The system receives up to 1M payment events/day in bulk feeds, all potentially targeting the same execution timestamp. Ingestion rate is 100-150 TPS. A bulk load of 500K events at 100 TPS all targeting the same time must be handled. The rate limiter divides time into fixed windows, assigns random-jittered slots within each window, and uses `SELECT FOR UPDATE NOWAIT` for lock-free concurrency control. The service exposes a REST API for slot assignment.

## Version Constraints

- **Quarkus**: 3.29.2
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
8. **Dynamic per-request search limit** — no global frontier or fixed MAX_LOOKAHEAD; PL/SQL skip query finds first OPEN window, search limit = `skipTarget + headroom`
9. **Window status column** (`OPEN`/`CLOSED`) on `rate_limit_window_counter` — enables O(log N) indexed skip queries via composite index `(status, window_start)`
10. **Proportional first-window capacity** — when `requestedTime` falls mid-window, first window's effective max = `floor(maxPerWindow * remainingTime / windowSize)` with constrained jitter `[elapsedMs, windowSizeMs)`

## Dynamic Per-Request Search Limit

Instead of a fixed `MAX_LOOKAHEAD` or a global frontier, the search limit is computed
dynamically per request inside the PL/SQL block:

```
Phase 1: Try first window with proportional capacity
  effectiveMax = floor(maxPerWindow * remainingMs / windowSizeMs)
  jitter constrained to [elapsedMs, windowSizeMs) so scheduledTime >= requestedTime

Phase 2 (if Phase 1 fails):
  skipTarget   = find_first_open_window(windowStart)  -- O(log N) indexed query
  searchLimit  = skipTarget + headroomSecs
  Walk from skipTarget to searchLimit
```

- **No global state**: No `AtomicReference`, no `@PostConstruct` initialization, no cross-request interference.
- **`headroom`**: Fixed configurable value (default: 100 windows). How far past the skip target we search.
- **Skip query**: Uses composite index `(status, window_start)` on `rate_limit_window_counter` to find the first `OPEN` window after the requested window in O(log N).

**Behavior**:
- First event: no windows exist → Phase 1 succeeds immediately.
- After 5,000 windows filled: skip query jumps to first OPEN window, then searches `headroom` windows from there.
- Events at different time horizons (e.g., near-term vs 1 year ahead) never interfere — each request computes its own search limit.
- No static ceiling that needs manual tuning. The skip query adapts to the actual window occupancy.

**Multi-node consistency**: No cross-node coordination needed. Each request independently queries the database for the first OPEN window. This is inherently consistent — all nodes see the same window status via the shared database.

## Capacity Planning for 500K Bulk Load

**Scenario**: 500K events at 100 TPS, all requesting the same `scheduledTime`.

| Parameter | Value | Rationale |
|---|---|---|
| Ingestion duration | 5,000s (~83 min) | 500K / 100 TPS |
| Windows needed | 5,000 | 500K / 100 per window |
| Time spread | ~5.5 hours | 5,000 x 4s |
| Per-request search | headroom = 100 windows | Skip query jumps past full windows |
| DB ops/sec (hot path) | ~300 | 100 TPS x 3 ops (SELECT FOR UPDATE + UPDATE + INSERT) |
| Lock hold time per tx | ~3-5ms | UPDATE counter + INSERT slot + commit |
| Effective lock utilization | ~50% at 100 TPS | 100 x 5ms = 500ms/sec of lock hold |

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
| 5 | `src/main/resources/db/migration/V1__rate_limiter_schema.sql` | Flyway DDL: 3 tables (window_counter with status column + composite index), indexes, constraints |
| 6 | `src/main/kotlin/com/ratelimiter/db/Tables.kt` | Exposed Table objects mirroring DDL exactly |
| 7 | `src/main/kotlin/com/ratelimiter/db/ExposedDatabaseInitializer.kt` | CDI bean connecting Exposed to Quarkus Agroal DataSource |

### Domain Models (Phase 3)

| # | File | Responsibility |
|---|---|---|
| 8 | `src/main/kotlin/com/ratelimiter/config/RateLimitConfig.kt` | Immutable domain model data class |
| 9 | `src/main/kotlin/com/ratelimiter/slot/AssignedSlot.kt` | Result type (`AssignedSlot` data class) |
| 9b | `src/main/kotlin/com/ratelimiter/slot/Exceptions.kt` | `SlotAssignmentException` + `ConfigLoadException` |

### Data Access (Phase 4)

| # | File | Responsibility |
|---|---|---|
| 10 | `src/main/kotlin/com/ratelimiter/config/RateLimitConfigRepository.kt` | Config CRUD + 5s ConcurrentHashMap cache |

### Core Algorithm (Phase 5)

| # | File | Responsibility |
|---|---|---|
| 11 | `src/main/kotlin/com/ratelimiter/slot/SlotAssignmentService.kt` | Two-phase slot assignment, proportional first-window, pre-computed jitter, FOR UPDATE NOWAIT, skip query, idempotency |

### REST API Layer (Phase 6)

| # | File | Responsibility |
|---|---|---|
| 12 | `src/main/kotlin/com/ratelimiter/api/SlotAssignmentResource.kt` | JAX-RS `@Path("/api/v1/slots")` for slot assignment |
| 13 | `src/main/kotlin/com/ratelimiter/admin/RateLimitAdminResource.kt` | JAX-RS `@Path("/admin/rate-limit")` for config CRUD + cache flush |
| 14 | `src/main/resources/application.yaml` | All config keys with defaults |

### Infrastructure (Phase 7)

| # | File | Responsibility |
|---|---|---|
| 15 | `docker-compose.yml` | Oracle 19c container (codeassertion/oracledb-arm64-standalone) |
| 16 | `scripts/setup-oracle.sh` | Bash script: start Oracle, wait for ready, create app user |

### Tests (Phase 8)

| # | File | Responsibility |
|---|---|---|
| 17 | `src/test/kotlin/com/ratelimiter/OracleTestResource.kt` | Testcontainers Oracle XE lifecycle manager |
| 18 | `src/test/kotlin/com/ratelimiter/RateLimitConfigRepositoryTest.kt` | Config insert/load/cache/deactivate tests |
| 19 | `src/test/kotlin/com/ratelimiter/SlotAssignmentServiceTest.kt` | Idempotency, window walk, jitter bounds, proportional first-window, skip query, status transitions, isolation |
| 20 | `src/test/kotlin/com/ratelimiter/ConcurrencyTest.kt` | 100 concurrent threads, no collisions, no deadlocks, counter consistency |

## Dependency Graph

```
Tables.kt ──────────────────────────────────────────┐
    |                                                |
    +-- RateLimitConfig.kt (domain model, no deps)   |
    +-- AssignedSlot.kt (result types, no deps)       |
    |                                                |
    +-- RateLimitConfigRepository.kt                 |
    |       depends on: Tables.kt, RateLimitConfig   |
    |                                                |
    +-- SlotAssignmentSql.kt                          |
    |       (PL/SQL constants, no deps)                |
    |                                                |
    +-- SlotAssignmentService.kt                     |
    |       depends on: SlotAssignmentSql,            |
    |         RateLimitConfig, AssignedSlot,           |
    |         ConfigRepository                        |
    |                                                |
    +-- SlotAssignmentResource.kt                    |
    |       depends on: SlotAssignmentService,        |
    |         AssignedSlot                           |
    |                                                |
    +-- RateLimitAdminResource.kt                    |
            depends on: RateLimitConfigRepository     |
```

## SlotAssignmentService — Core Algorithm

```
assignSlot(eventId, configName, requestedTime):
  1. loadActiveConfig(configName) -> from cache or DB
  2. windowStart = alignToWindowBoundary(requestedTime, config.windowSizeSecs)
     elapsedMs = Duration.between(windowStart, requestedTime).toMillis()
     effectiveMax = computeEffectiveMax(maxPerWindow, elapsedMs, windowSizeMs)
     firstJitterMs = ThreadLocalRandom.nextLong(elapsedMs or 0, windowSizeMs)
     fullJitterMs = ThreadLocalRandom.nextLong(0, windowSizeMs)
     headroomSecs = headroomWindows * config.windowSizeSecs
  3. Execute single PL/SQL block via CallableStatement (10 IN, 5 OUT params):
     -- Server-side (inside PL/SQL anonymous block):
     a. check_existing_slot: SELECT by event_id -> if found, ou_status = EXISTING
     b. Phase 1: Try first window with pre-computed proportional capacity
        - try_lock_window(windowStart) -> if count < in_effective_max:
          claim_slot_in_window(windowStart, count, in_first_jitter_ms)
     c. Phase 2 (if Phase 1 fails): Skip to first OPEN window
        - skipTarget = find_first_open_window(windowStart) -> O(log N) indexed query
        - searchLimit = skipTarget + headroomSecs
        - Walk loop (current_window from skipTarget to searchLimit):
          try_lock_window -> SELECT FOR UPDATE NOWAIT
          -> SQLCODE -54/-60 -> skip to next window
          -> if slot_count >= max_per_window -> skip
          -> claim_slot_in_window(window, count, in_full_jitter_ms)
            INSERT slot + UPDATE counter (atomic status OPEN/CLOSED transition)
            -> DUP_VAL_ON_INDEX on event_id -> idempotency race -> ou_status = EXISTING
     d. If no window had capacity -> ou_status = EXHAUSTED
     e. Marshal ou_ locals to OUT bind variables
  4. Interpret result (pure Kotlin):
     - NEW: return AssignedSlot
     - EXISTING: return AssignedSlot (idempotent hit)
     - EXHAUSTED: throw SlotAssignmentException
```

**Key design features**:
- Entire slot assignment (idempotency check + two-phase window search + lock + insert + counter update) in a **single PL/SQL anonymous block** — one JDBC round trip
- **Two-phase approach**: Phase 1 tries first window with proportional capacity; Phase 2 skips to first OPEN window
- **No global frontier**: Search limit computed per-request inside PL/SQL (`skipTarget + headroom`)
- **Status-based skip query**: `find_first_open_window()` uses composite index `(status, window_start)` for O(log N) lookups
- **Proportional first-window**: `effectiveMax = floor(maxPerWindow * remainingMs / windowSizeMs)` pre-computed in Kotlin
- **Pre-computed jitter**: Both jitter values computed in Kotlin via `ThreadLocalRandom` and passed as IN params — no `DBMS_RANDOM` in PL/SQL
- **Constrained first-window jitter**: `firstJitterMs = ThreadLocalRandom.nextLong(elapsedMs, windowSizeMs)` ensures `scheduledTime >= requestedTime`
- **Atomic status transition**: `claim_slot_in_window` sets `status = CLOSED` when `slot_count + 1 >= maxPerWindow`
- Counter row creation uses plain `INSERT` (with `status = 'OPEN'`) + catch DUP_VAL_ON_INDEX
- `window_size` stored as ISO-8601 Duration string (e.g., `"PT4S"`) instead of integer seconds

## Testing Strategy

- **Unit-like tests** (with Testcontainers Oracle XE): SlotAssignmentService is deeply coupled to Oracle SQL semantics, so tests use real Oracle via Testcontainers rather than mocks
- **Config repository tests**: Insert/load/cache TTL/deactivate
- **Proportional first-window tests**: Verify proportional max, constrained jitter (`scheduledTime >= requestedTime`), overflow to next window
- **Skip query tests**: Verify status-based skip avoids walking full windows, status transitions to CLOSED
- **Isolation tests**: Verify far-future events don't corrupt near-term search (no global frontier)
- **Concurrency test**: 100 threads, same target time, verify:
  - No window exceeds `max_per_window`
  - Total slots == total events submitted
  - Counter values match actual slot counts per window
  - No deadlocks or unhandled exceptions
  - Idempotency: same `event_id` 10x returns identical `AssignedSlot`

## Verification

1. **Build**: `./gradlew build` — compiles, runs all tests
2. **Start Oracle**: `./scripts/setup-oracle.sh`
3. **Run app**: `./gradlew quarkusDev`
4. **Seed config**: `curl -X POST localhost:8080/admin/rate-limit/config -H 'Content-Type: application/json' -d '{"configName":"default","maxPerWindow":100,"windowSize":"PT4S"}'`
5. **Test slot assignment**: `curl -X POST localhost:8080/api/v1/slots -H 'Content-Type: application/json' -d '{"eventId":"test-1","configName":"default","requestedTime":"2025-06-01T12:00:00Z"}'`
6. **Run tests**: `./gradlew test` (requires Docker for Testcontainers)
