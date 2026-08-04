# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

taskmesh is a reusable, multi-node **task orchestration engine**: claim-based,
DB-coordinated, shared-nothing across nodes. A "task" is `(task_type, task_id, payload)`.
The engine (feeder, claim/lease, retry, pause, telemetry) is domain-agnostic; all domain
work lives behind a consumer-supplied `TaskProcessor`, registered by `task_type`. taskmesh
never depends on a consumer — consumers depend on it.

It is a **library, not an application** — there is no `main`, no runnable server. It is
consumed as Gradle/Maven artifacts (`com.palmyralabs.taskmesh:*:0.1.0-SNAPSHOT`).

## Build & test

All Gradle commands run from the **`source/`** directory (that's where the wrapper and
`settings.gradle` live), not the repo root. Java 21 toolchain — if no JDK 21 is installed locally,
Gradle auto-downloads one via the foojay resolver plugin (configured in `settings.gradle`), so the
first build needs network access.

```bash
cd source
./gradlew build                 # compile + test everything (gradlew.bat on Windows)
./gradlew :taskmesh-simple:test # test one module (or :taskmesh-pro:test / :taskmesh-max:test)
./gradlew :taskmesh-engine:test # etc.

# Run a single test class / method (JUnit 5):
./gradlew :taskmesh-simple:test --tests 'com.palmyralabs.taskmesh.simple.MultiNodeTaskLedgerTest'
./gradlew :taskmesh-simple:test --tests '*MultiNodeTaskLedgerTest.claimSingleWinner'
```

Tests use JUnit 5 + Lombok. JPA tests (`taskmesh-simple`/`-pro`/`-max`) run against
in-memory **H2**, not Postgres — see the Postgres note below for behaviors H2 does not catch.

## Module architecture

Dependency direction is strictly one-way; `-api` is the stable core everything compiles against.
Persistence is **three fully-independent tiers** (D31) rather than one `taskmesh-impl` — a
consumer depends on exactly one of them, never more than one.

```
consumer's TaskProcessor impls
        │  ▼
  taskmesh-engine ──▶ taskmesh-api         (plain Java, ExecutorService-based; NO Spring, NO JDBC)
        ▲                  ▲
  taskmesh-simple ─────────┤  ┐
  taskmesh-pro ────────────┤  ├─ pick exactly one (own JPA entities/repos/DDL each, D31)
  taskmesh-max ────────────┘  ┘
        ▲
  taskmesh-spring-boot-starter             (tier-agnostic: TaskmeshProperties/WorkloadProfile/
        ▲                                   Slf4jMetricsReporter — depends on NO tier, D31)
  taskmesh-testkit ──▶ taskmesh-api        (in-memory fakes of every interface, for unit-testing a processor)
```

- **`taskmesh-api`** — interfaces + domain types only, JDK + SLF4J. The extension-seam
  interfaces (`TaskProcessor`, `TaskContext`, `ClaimStrategy`, `TaskLedger`, `BacklogPromoter`,
  `HeartbeatStrategy`, `FailureClassifier`, `RetryPolicy`, `HistoryWriter`, `AuditEventSink`,
  `IncidentStore`, `PauseController`, `ProcessorRegistry`, `PayloadProvider`, `TimeoutPolicy`,
  `MetricsReporter`, `TransactionRunner`) live here, plus tier-agnostic shared types like `Jitter`,
  `AttemptCounters` (D33) and `TaskInfo`/`TerminalContext`/`TerminalOutcome` (D34 — `TaskContext`
  and `TerminalContext` both extend `TaskInfo`, and stay separate types on purpose).
- **`taskmesh-engine`** — the one "driving implementation": feeder/poller, claim loop,
  dispatcher, worker pool, and the `BacklogPromoter` promotion scheduler (its own
  `ScheduledExecutorService`, mirrors the claim poller). `TaskMeshEngine` (interface, built via
  `TaskMeshEngine.builder()`) is the public entry point; `TaskMeshEngineImpl` and everything else
  lives under `engine.impl` and must not be referenced directly. **The engine never branches on
  single-node vs multi-node, or on which tier is wired** — topology and hot-row-creation timing
  are entirely `ClaimStrategy`/`HeartbeatStrategy`/`TaskLedger`/`BacklogPromoter` implementation
  concerns. Zero Spring dependency.
- **`taskmesh-simple` / `taskmesh-pro`** — JPA-backed implementations (`Jpa*`,
  `MultiNodeTaskLedger`, `SimpleTaskLedger`) of the persistence contracts, each over its own copy
  of the seven `tm_*` tables (own entities, repositories, DDL — deliberately duplicated, not
  shared, per D31). `-simple` (small-scale, single-node default) and `-pro` (medium/large,
  multi-node default) both always wire `NoopBacklogPromoter` (eager hot-row creation, unchanged
  D28 behavior).
- **`taskmesh-max`** — large-scale/partitioned tier. Ships only `PartitionedTaskLedger` (no
  `SimpleTaskLedger` — partitioning is a multi-node/scale concept). `enqueue()` only writes
  `tm_task`, deferring hot-row creation to the wired `BacklogPromoter` (`NoopBacklogPromoter`
  still the default; `JpaBacklogPromoter` opt-in, throttles hot-row creation to bounded batches
  via `promoted_at`). `partitionKey` (on the shared `TaskRef`, D32) is caller-supplied and
  required at this tier — `enqueue()`/`rerun()` throw on a missing one, no default bucket. Five
  tables (`tm_task`/`tm_in_flight`/`tm_history_event`/`tm_incident`/`tm_attempt_history`) carry
  `partition_key`; `tm_audit_event` is deferred. taskmesh ships zero partitioning DDL opinion —
  see D32.
- **`taskmesh-spring-boot-starter`** — optional, and **tier-agnostic** (D31): just
  `@ConfigurationProperties` (`TaskmeshProperties`, `taskmesh.*`), `WorkloadProfile`,
  `Slf4jMetricsReporter`, `SingleProviderPayloadProviderFactory`. Zero dependency on any tier —
  each tier depends on *it*, not the other way around, and ships its own
  `<tier>.starter.{ProfileFactories,TaskmeshJpaAutoConfiguration,TaskmeshEngineAutoConfiguration,
  TaskmeshProfileBuilder}` plus its own `AutoConfiguration.imports`. `ProfileFactories` is the
  interfaces→implementations mapping (per `WorkloadProfile`) shared by a tier's auto-config and
  its manual `TaskmeshProfileBuilder`, so the two paths never drift. No `@ConditionalOnClass`
  tier-detection needed — whichever one tier module a consumer depends on is the only one whose
  `AutoConfiguration.imports` is ever on the classpath.
- **`taskmesh-testkit`** — in-memory `InMemory*`/`Fake*` impls of every `-api` interface
  (including `InMemoryBacklogPromoter`) for unit-testing a `TaskProcessor` with no DB and no
  Spring context.
- **`taskmesh-integration-test`** — test-only module (no published artifact). Boots the whole
  engine via the Spring Boot starter (currently against `taskmesh-simple`) against in-memory H2
  and drives real enqueue → claim → process → release / incident / rerun flows end-to-end
  (`EngineEndToEndTest`).

## Non-negotiable design invariants

These are load-bearing; violating one is a correctness bug, not a style choice.

- **Claim is the only cross-node gate** — one atomic, `WHERE`-guarded UPDATE. 0 rows affected =
  lost the race, back off silently (not logged above debug). No secondary lock table, no external
  coordinator (no ZooKeeper/Redis).
- **At-least-once, never exactly-once** — the engine guarantees at-least-once dispatch. Every
  `TaskProcessor` must be idempotent on its stable `task_id` (owns its own dedup); the engine
  provides no completion ledger.
- **Failures are classified, not lumped** — `FailureClassifier` decides retryable (default) vs
  non-retryable. The **retry-exhaustion check lives inside `TaskLedger`/`ClaimStrategy.release()`**
  (atomic with the retry-counter read), not in `RetryPolicy` or `FailureClassifier` (D16).
- **Two nested counters, not one** (D33) — `attempt_count` is the *outer*, 1-based, unbounded count
  of operator-initiated runs (only `rerun` moves it); `retry_count` is the *inner*, 0-based ordinal
  of the execution running right now within that attempt, bounded by `max_retries` and **reset to
  `-1` on every `rerun`**, so a re-run of an exhausted task gets a full fresh budget.
- **Retry budget is spent at `claim()`, not `release()`** (D33) — every claim bills `retry_count +1`
  **except** one resuming a `YIELDED` row, and the ledger terminates the task right there
  (`ClaimOutcome.EXHAUSTED`, cold snapshot `FAILED`, hot row deleted) rather than admitting an
  over-budget execution. This is the only thing that makes a lost lease cost anything: the node
  that lost it is unfenced by construction and can never write, so a release-only model let a task
  that never finishes inside one lease be reclaimed forever. `release()` keeps its own check for
  the ordinary path but **never moves a counter**. Release dispositions are
  `COMPLETED`/`FAILED_TERMINAL`/`RETRY`/`YIELD`.
- **`ClaimStrategy` opens no transaction; the caller does** (D34) — `claim`/`renew`/`release` run
  inside the caller's boundary, supplied by the engine via `TransactionRunner` (an `-api` interface,
  so `taskmesh-engine` keeps its zero-Spring dependency; each tier adapts its own
  `TransactionTemplate`, whose `REQUIRED` propagation makes the ledger's work *join* rather than
  commit separately). That is what lets a terminal write and the consumer's `onTerminal` commit as
  one unit. Calling a `ClaimStrategy` without an ambient transaction now fails loudly.
  `TaskLedger.enqueue`/`rerun`/`recoverOwnClaims` keep their own boundaries — `enqueue`'s
  duplicate-race swallow only works while its failed insert is isolated.
- **`TaskProcessor` has two lifecycle hooks, both default no-ops** (D34) — `onTerminal` fires once
  per terminal transition (all four paths), *inside* the terminal transaction, so a consumer's
  derived projection cannot diverge from the queue; throwing vetoes the transition, which means a
  broken callback presents as tasks that never finish. `onStart` fires before every execution and
  is how a consumer spots an operator rerun: `retryCount == 0` with `attemptCount > 1` and nothing
  else. There is deliberately no `onRerun` — the D33 counters already carry the signal.
- **One lock order, everywhere: hot row, then cold** (D34) — every write path in every ledger
  touches `tm_in_flight` before `tm_task`. Forced by D33's claim-time termination, which holds the
  hot row from the claim UPDATE before it can know the task is terminal, so it can never take the
  cold row first. `release` used to open with `lockByPk(tm_task)`; against real Postgres that
  deadlocked with the claim path 40 times out of 40 (H2 cannot show this — `@DataJpaTest` rolls
  everything back on one connection, so there is no second transaction to deadlock against).
  Adding a path that writes `tm_task` first would reintroduce it.
- **A missing processor or node crash does NOT burn a retry attempt** — it's a
  deployment/liveness issue, not a task failure; the claim is released for another node.
- **Silence is a bug** — unregistered task types, lost claims, classification gaps must be loud
  (logged / incident-worthy). Duplicate `taskType()` registration throws at construction, refusing boot.
- **`@Transactional` on `process()` is silently ignored** — the engine calls `process()` via a
  plain method call, never a Spring proxy. Demarcate transactions explicitly inside processor code.
- taskmesh is **not** a workflow/BPMN engine, message queue, or cron scheduler. No step graphs,
  no pub/sub, no time-based triggering — tasks are enqueued externally and pulled from a DB table.

## The `tm_task` / `tm_in_flight` split (most important schema fact)

`tm_task` (cold) and `tm_in_flight` (hot) are **a pair** — one row in each per enqueued task,
both created eagerly at enqueue, until a terminal outcome deletes the `tm_in_flight` row (D28).

**The hot row is the sole source of truth for query/claim/release** — those operations read only
`tm_in_flight` and never join to `tm_task`.

- **`tm_in_flight`** carries all claim-driving state: `status` (`QUEUED` reserved to `node_id` /
  `NULL` unreserved / `CLAIMED` / `RELEASED`), `node_id`, the live nested counters
  (`attempt_count`/`retry_count`, bounded by `max_retries` — D33), the backoff gate
  (`next_retry_at`), and `lease_expires_at`.
  Deleted at terminal outcome, so it tracks current backlog+active, not all-time history.
- **`tm_task`** is written only at enqueue (`QUEUED`) and, as a **cold snapshot**, on the terminal
  transition — where the final `attempt_count`/`retry_count`/`max_retries` are **copied from the hot
  row** before it's deleted, alongside terminal `status`/`reason_code`. A retry release does *not*
  touch it. **No payload column** — payload is the consumer's `PayloadProvider` concern, never
  stored here.

`MultiNodeTaskLedger` uses a **4-window** eligibility model — reservation / reclaim / affinity /
claim-timeout. A freshly enqueued `QUEUED` row is reserved to the enqueuing node; another node may
steal it only after `reservationSeconds`. `findClaimableMultiNode` (discovery, which also enforces
the `next_retry_at` backoff) and `claimMultiNode` (claim) **must stay in exact sync** on that
eligibility block — a past bug leaked the short reclaim window into the `WHERE` clause. See
`docs/schema.md` for the full model. `SimpleTaskLedger` (BATCH profile, single-node) skips
lease/affinity/claim-timeout/reservation entirely.

The seven-table reference (`tm_task`, `tm_in_flight`, `tm_history_event`, `tm_attempt_history`,
`tm_audit_event`, `tm_incident`, `tm_control_flag`) is documented in `docs/schema.md`, with a
ready-to-run **Postgres reference DDL at `docs/ddl/postgres/taskmesh-schema.sql`**. taskmesh itself
still ships no DDL on the classpath — Hibernate runs with `ddl-auto=validate` and each consuming app
owns its `dbscripts/` (copy/adapt the reference script). The tests use H2 with `ddl-auto=create-drop`.

### Postgres note

`TEXT`-like columns (`error_summary`, `detail`, `message`) map with
`@JdbcTypeCode(SqlTypes.LONGVARCHAR)`, **not `@Lob`** — `@Lob` on a `String` becomes Postgres `oid`,
not `text` (Hibernate 6 pitfall). H2 (used in tests) has a native CLOB and won't surface this, so
verify `TEXT`-column changes against real Postgres.

## Docs

`docs/planning/` holds the authoritative design discussion (`overview.md`, `interfaces.md`,
`runtime-flow.md`, `module-layout.md`, `claim-strategy.md`, `decisions.md` with numbered `D*`
decisions, `profiles.md`). `docs/schema.md` documents the actual `tm_*` tables. `docs/planning/` describes intent; when it disagrees with `source/`, trust the
code and `docs/schema.md`.
