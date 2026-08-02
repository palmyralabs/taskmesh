# Infra implementation specs

For each infra interface (see `interfaces.md`), two candidate implementations
are specified — one drawing from nm_cleanser's proven pattern, one from
palmyra76/workflow's. Where a peer project has no real prior art for an
interface, that's called out explicitly rather than forcing a fake second
variant. This is spec-level (schema, config, behavior) — no code yet.

The two variants per interface are not usually an either/or choice for the
whole project: they're two implementations of the same interface, selected
per deployment (e.g. single-node dev vs multi-node production) or even per
`task_type` where the interface is parameterizable that way.

---

## ClaimStrategy

**Full detail (interface contract, exact SQL, schema, edge cases, config) now
lives in [`claim-strategy.md`](claim-strategy.md) — the summary below is kept
short intentionally.**

### Variant A — `SingleNodeClaimStrategy` (cleanser-inspired)

For single-node deployments (local dev, `taskmesh-devtools`, low-volume
consumers) — no cross-node race is possible, so no lease is needed.

- Claim: `UPDATE task SET status='CLAIMED', node_id=:me WHERE task_id=:id AND status='QUEUED'` — optimistic WHERE-guard only, no lease/expiry columns needed.
- Recovery: on node startup, find tasks this node left `CLAIMED` from a prior
  run and requeue them (mirrors nm_cleanser's JVM-restart recovery — since
  there's only ever one node, "this node's stale claims" == "all stale claims").
- No config knobs beyond none — there's no lease to size.

### Variant B — `LeaseClaimStrategy` (workflow-inspired)

For multi-node production. This is task-execution.md's `in_flight` design,
generalized to `task_id`/`task_type`.

- Table: `task_claim(task_id, task_type, node_id NULL, claimed_at, lease_expires_at)`.
- Claim: `UPDATE task_claim SET node_id=:me, claimed_at=now(), lease_expires_at=now()+:lease WHERE task_id=:id AND (node_id IS NULL OR lease_expires_at < now())`.
- Recovery: visibility-based, no sweeper (D3) — a live node's feeder naturally
  re-sees a dead node's expired claim.
- Config: `lease.durationSeconds` (default 300, from task-execution.md). The
  richer 3-window model (reclaim/affinity/timeout, from workflow) is a
  possible future config shape for this same implementation — not a third
  variant, just a richer config surface on Variant B, deferred until real
  multi-node contention data justifies it (see open-questions.md history).

**Default**: `LeaseClaimStrategy` in `taskmesh-postgres`; `SingleNodeClaimStrategy`
in `taskmesh-testkit`/`taskmesh-devtools`.

---

## TaskQueue (optional extension — see interfaces.md for the full rationale)

**Not required by any default implementation.** Split out of
`SchedulingStrategy` purely as an opt-in seam for the `HIGH_VOLUME` profile,
in case `tm_task`'s partial-index scan ever proves insufficient at real
backlog depth. Every default `SchedulingStrategy` variant (both `BATCH`'s
and `HIGH_VOLUME`'s) queries `tm_task` directly, exactly as `claim-strategy.md`
already specifies, with no `TaskQueue` indirection. `PostgresTaskQueue` is
the only implementation specified today, and it's just that same query
given an interface name — nothing changes unless a consumer actually needs
to swap backlog storage. No second implementation is designed; see the open
item in `interfaces.md`.

## SchedulingStrategy (feeder)

### Variant A — `FixedIntervalSchedulingStrategy` (cleanser-style)

- One `@Scheduled` poll every N seconds (default 5s, configurable). Counts
  current in-progress tasks, fetches up to `(cap − inProgress)` `QUEUED` rows,
  submits directly.
- Simple; fine for low-to-moderate throughput or single-node deployments.

### Variant B — `QueueBackedSchedulingStrategy` (workflow-style)

- One feeder thread per node, fetches a batch (default 500, from
  task-execution.md) into an in-memory bounded queue. Submit-direct when
  pool capacity allows; **blocking submit** when the pool's queue is full
  (task-execution.md's custom `RejectedExecutionHandler` doing `queue.put()`
  — never `CallerRunsPolicy`). A periodic safety-net re-poll (default 30s,
  workflow-style) catches anything that fell through a claim race.
- Higher throughput; matches this org's horizontal-scalability requirement.

**Default**: `QueueBackedSchedulingStrategy` in `taskmesh-postgres`/`-engine`
for production; `FixedIntervalSchedulingStrategy` as the simpler alternative.

---

## HeartbeatStrategy

This directly resolves Q-HEARTBEAT by not forcing a single answer — both
variants are offered, consumer picks per deployment.

### Variant A — `ExplicitHeartbeatStrategy` (cleanser-style)

- A separate, frequent (≤30s) heartbeat call, distinct from lease renewal.
- A task whose last heartbeat exceeds a threshold (e.g. 2× expected interval)
  without progress is flagged — written to `IncidentStore`, not silently
  ignored (unlike nm_cleanser's own documented gap, where nothing currently
  *acts* on a missed heartbeat).
- Higher DB/network chatter; buys faster hang detection.

### Variant B — `UnifiedRenewalHeartbeatStrategy` (workflow-style, corrected)

- No distinct heartbeat call — renewing the claim (`ClaimStrategy` renewal)
  *is* the heartbeat.
- **Renewal is checkpoint-triggered, not a fixed background timer.** Earlier
  draft of this spec described renewal as firing "on the lease-derived
  cadence (every `lease/2`)" as an independent scheduled tick — that's wrong,
  and reintroduces a real risk: a fixed timer can miss a single scheduled
  tick under a transient stall (GC pause, slow I/O) and lose an otherwise
  healthy claim. task-execution.md's actual mechanism (verbatim): *"the
  per-document gate checks ownership; past 50% of the lease it becomes a
  renewing UPDATE."* Renewal happens at every `TaskContext` checkpoint call
  (whatever a `TaskProcessor` calls at its natural progress boundaries —
  per-document, per-chunk, whatever granularity the processor defines) once
  past 50% of the lease has elapsed — not on a schedule independent of
  whether work is actually happening.
- Consequence: a task's total wall-clock duration has no fixed ceiling as
  long as it keeps making forward progress — only a task making **zero**
  progress for a full lease window loses its claim, which is indistinguishable
  from a genuine hang and correctly should lose it. No livelock risk for
  slow-but-progressing task types.
- No separate hang-detection watchdog; staleness is purely
  zero-progress-for-one-lease-window-driven, not wall-clock-driven.

**Default recommendation**: `UnifiedRenewalHeartbeatStrategy` for v1 (per
D5's lean) — `ExplicitHeartbeatStrategy` available for task types where fast
hang detection genuinely matters (e.g. a task type with long, silent
external-API calls where a 5-minute lease-expiry wait would be too slow).

---

## PauseController

### Variant A — `ClusterFlagPauseController` (cleanser + task-execution.md style)

- Single DB flag row, cluster-wide scope only. The node receiving the pause
  API call flips its in-memory state immediately; other nodes poll at a
  fixed interval (default 15s, from task-execution.md). `TaskContext.isPauseRequested()`
  reads the in-memory flag — no DB read per task-level check.
- Covers the non-negotiable "pause the whole engine" requirement.

### Variant B — `ScopedPauseController` (workflow-style)

- Two independent scopes on top of Variant A's mechanism: `task_type`-level
  suspend (blocks new claims of that type; in-flight tasks of that type
  unaffected) and individual `task_id`-level cancel (targeted stop of one
  task — synchronous only, mirroring what workflow *actually* ships, not
  its unbuilt async-cancel design flagged as a gap in the comparison sheet).
- Extra table: `pause_scope(scope_type ['TASK_TYPE'|'TASK'], scope_key, requested_at, requested_by)`.

**Default**: `ClusterFlagPauseController` is the v1 requirement; `ScopedPauseController`
is an additive enhancement (built as a layer on top, not a replacement) for
consumers wanting finer control (e.g. pause only `POLICY_MIGRATE` while
`VERIFY` keeps running).

---

## HistoryWriter

### Variant A — `AttemptHistoryWriter` (cleanser-style, `exec_proc_run` pattern)

- One row per `(task_id, attempt_no)`: status, `started_at`, `ended_at`,
  items processed/failed, error summary. One INSERT at attempt start, one
  UPDATE at attempt end.
- Simple; sufficient when per-event granularity isn't needed.

### Variant B — `LeveledEventHistoryWriter` (workflow-style)

- Append-only event table: `event_type` (STARTED/COMPLETED/FAILED/RETRIED/
  HEARTBEAT/...), `task_id`, `attempt_no`, detail, `occurred_at`.
- Verbosity level (`NULL`/`MINIMAL`/`STANDARD`/`FULL`, default `STANDARD`)
  controls which event types get persisted — a knob to trade granularity
  for write volume at scale.
- Buffered per attempt, flushed atomically with the attempt's terminal write.
- **Rule carried over**: history is audit-only — retry/claim decisions never
  read from it.

**Default**: `LeveledEventHistoryWriter` for `taskmesh-postgres` (matches
this org's Kafka-durable-audit-log goal — a Kafka-backed implementation of
the same interface is a plausible later addition, not a third variant);
`AttemptHistoryWriter` as the lightweight alternative.

---

## AuditEventSink

### Variant A — `SimpleAuditEventSink` (cleanser-style, `exec_event_log` pattern)

- `event_type`, `task_id` (nullable — some events are cluster-wide, e.g.
  cluster pause), free-text `message`, `occurred_at`, free-text `performed_by`.

### Variant B — `ActorTaggedAuditEventSink` (workflow-style)

- Adds a typed `actor_type` enum (`APPLICATION_USER` / `SYSTEM` /
  `ENGINE_MODULE`) alongside `performed_by`, so every event is attributable
  to a category, not just a name string.

**Default recommendation**: `ActorTaggedAuditEventSink`, given the IRDAI-grade
audit requirement in the originating consumer — "who/what did this" typing
is not optional there. `SimpleAuditEventSink` remains available for
consumers without that requirement.

---

## IncidentStore

### Variant A — `ImplicitIncidentStore` (cleanser-style — cleanser has no incident concept at all)

- No dedicated table or write path. "Incidents" are a read-only query over
  the task table: `WHERE status = 'FAILED'` (or classified non-retryable).
  No `resolved_at` tracking beyond the task's own state.
- Cheapest possible option for consumers who don't need a dedicated ops queue.

### Variant B — `DedicatedIncidentStore` (workflow-style, `pmop_incident` pattern)

- Real table: `incident_id, task_id, task_type, reason_code, message,
  created_at, resolved_at, resolved_by`. `reason_code` comes from
  `FailureClassifier`'s typed enum.
- Written in an isolated transaction so the incident record survives the
  failing transaction's rollback (matches workflow's `REQUIRES_NEW` pattern).

**Default recommendation**: `DedicatedIncidentStore` — pairs naturally with
D6 (adopting the retryable/non-retryable classification); a queryable "what's
broken right now" view is the whole point of closing that gap.
`ImplicitIncidentStore` remains available as a minimal fallback.

---

## MetricsReporter

**Deviates from the two-peer-variant pattern deliberately**: neither
nm_cleanser nor palmyra76/workflow has a working metrics layer (both
declare Actuator/Micrometer as unused or entirely absent — see the
comparison sheet, Developer experience → Observability/metrics row). There
is no cleanser-style or workflow-style pattern to draw two real variants
from here; this is fresh design territory for both peers, not just for us.

### Variant A — `NoOpMetricsReporter` (default)

- Zero overhead. Matches both peer projects' actual current state.

### Variant B — `MicrometerMetricsReporter` (`taskmesh-metrics-micrometer`)

- Per-`task_type` counters (claims, completions, retries, failures-by-reason-code)
  and a duration histogram/timer per attempt.
- The one genuinely new piece of infra taskmesh adds that neither peer project has.

**Default**: `NoOpMetricsReporter` unless `taskmesh-metrics-micrometer` is
on the classpath, in which case `MicrometerMetricsReporter` auto-configures.
