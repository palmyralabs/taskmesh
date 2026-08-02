# Interfaces

Draft decomposition. Each section: responsibility, what it's NOT responsible
for, candidate implementations, and what's still open. This is the working
doc for the interface-by-interface discussion — status markers:
**[draft]** sketched but unreviewed, **[open]** genuinely undecided,
**[stable]** discussed and settled.

---

## TaskProcessor  — [draft]

**The** extension point. One implementation per `task_type`, supplied by the
consuming project.

- Responsible for: the actual domain work.
- Not responsible for: claiming, retry scheduling, lease renewal, pause
  propagation — the engine drives all of that around a `process()` call.
- Must: be idempotent (re-running on the same `task_id` skips already-done
  sub-work); check `TaskContext` ownership/pause signals at safe checkpoints.

**Candidate implementations**: consumer-supplied, per task_type. E.g. (in the
originating toolkit, not in taskmesh itself) `PolicyMigrationProcessor`.

**Open**: exact method signature — one `process(TaskContext ctx)` returning a
result/outcome type, or a richer contract split like workflow's
`activate/complete/terminate`? Leaning toward the single-method flat contract
(matches D2 — flat unit of work, no multi-step sequencing in core).

---

## TaskContext — [draft]

The toolbox passed into `TaskProcessor.process()`.

- Responsible for: exposing `taskId`/`taskType`/`payload`, ownership/pause/
  shutdown checks, progress/heartbeat reporting, error logging for sub-items.
- Not responsible for: giving access to domain repositories — processors
  inject their own (nm_cleanser's rule: keep the context narrow).

**Candidate implementations**: one real impl in `-engine`, one fake/hand-buildable
impl in `-testkit` for unit tests.

**Open**: exact method list. Working sketch:
`taskId() / taskType() / payload() / isOwnershipValid() / isPauseRequested() /
isShutdownRequested() / progress().report(...) / errorLogger().log(...)`.

---

## ClaimStrategy — [stable] — see [`claim-strategy.md`](claim-strategy.md) for full detail

Owns cross-node race arbitration, claim timing, and dead-worker recovery
together (deliberately aggregated — see the reasoning in the prior design
discussion: these three always have to agree with each other).

- Responsible for: atomic claim (exactly one winner determined atomically;
  a losing attempt returns a miss outcome, never an error), lease/claim
  expiry, renewal.
- Not responsible for: deciding *when* to renew (that's driven by `-engine`'s
  scheduled tick, informed by `HeartbeatStrategy` per Q-HEARTBEAT).

**Candidate implementations**: `taskmesh-postgres` (the reference/default,
matches task-execution.md's `in_flight` table design). A `taskmesh-testkit`
in-memory implementation for tests. No second production implementation
planned — see D3 (shared-table design is the only one under consideration).

**Open**: single lease window (task-execution.md's 5min/half-time model) vs.
workflow's three-window model (reclaim/affinity/timeout) — see the claim/lease
timing row in the comparison. Not yet decided; current lean is single window,
richer model deferred until real multi-node contention data exists.

---

## RetryPolicy — [draft]

Owns retry budget and backoff shape, parameterized per `task_type`.

- Responsible for: `maxAttempts`, backoff curve (delay given attempt number).
- Not responsible for: deciding whether a given failure is retryable at all
  — that's `FailureClassifier`.

**Candidate implementations**: `ExponentialBackoffRetryPolicy` (default,
matches task-execution.md: 2min × 2^(n-1), capped 1h, max_attempts=10) as a
`taskmesh-engine`-provided default; consumers can supply per-task_type
overrides.

**Open**: does a node crash (vs. an observed application failure) burn a
retry attempt? workflow makes this explicit (crash never burns a retry) —
task-execution.md's answer is implicit and needs to be made explicit here
(see comparison sheet, Failure & retry → Retry budget & backoff row).

---

## FailureClassifier — [draft]

Single chokepoint: is this failure retryable or terminal?

- Responsible for: classifying an exception into retryable (default) or
  non-retryable (explicit opt-in), with a typed reason code.
- Not responsible for: retry scheduling itself (`RetryPolicy`'s job) or
  logging (`AuditEventSink`/`IncidentStore`'s job).

**Candidate implementations**: a default classifier recognizing a
`NonRetryableException` type (mirrors workflow's pattern) carrying a typed
reason enum; consumers can supply additional classification rules.

**Open**: shape of the reason-code enum — start minimal (`CONFIG_ERROR`,
`TARGET_UNAVAILABLE`, `PERMANENT_DATA_ERROR`, `UNKNOWN`) and grow by demand,
or attempt a fuller taxonomy up front? Leaning minimal-and-grow.

---

## TaskQueue — [draft] — optional extension, not required by every implementation

Split out of `SchedulingStrategy` (see below) once it became clear that
"where the backlog physically lives" is a separate concern from "when/how
often to poll it" — the latter doesn't care whether the former is a Postgres
table or something else, and at `HIGH_VOLUME`'s scale (tens of millions of
backlog rows, see `profiles.md`) that storage choice is exactly the kind of
thing that might need to vary independently of everything else.

**This interface is opt-in, not load-bearing.** The default
`SchedulingStrategy` implementations query their backing store directly
without this indirection — no `TaskQueue` required for `BATCH` or
`REACTIVE`, and not required for `HIGH_VOLUME` either unless/until the
default backlog-storage approach actually proves insufficient at real
depth (see the relevant implementation doc for what "default" means
concretely). `TaskQueue` exists purely as a seam a `SchedulingStrategy`
implementation *may* delegate to when backlog storage genuinely needs to be
swappable — most deployments will never touch it.

- Responsible for: persisting the backlog of not-yet-claimed work, and
  answering "give me up to N claimable candidates" for a given `task_type`.
- Not responsible for: claim arbitration (`ClaimStrategy`), or deciding
  cadence/batch-size policy (`SchedulingStrategy` — see below).

**Candidate implementations**: `PostgresTaskQueue` (default) — named as its
own interface implementation instead of being implicitly bundled into the
feeder; see `claim-strategy.md`/`infra-implementations.md` for its actual
query behavior. A second, generic implementation (backing store TBD — a
sharded/partitioned table, or an external durable queue) is an explicit
**extension point**, not yet designed — see `profiles.md`'s `HIGH_VOLUME`
row and the open item below.

**Open**: at what backlog size does `PostgresTaskQueue`'s partial-index scan
stop being sufficient for `HIGH_VOLUME`'s sustained 334–668 completions/sec
floor (see the `profiles.md` discussion)? No number picked yet — needs load
testing against a realistic backlog, not a guess. Until that's known, a
second `TaskQueue` implementation stays a named extension point, not a
committed design.

---

## SchedulingStrategy (feeder) — [draft]

Decides when/how a node's feeder pulls from `TaskQueue` and stages claimable
work for `ClaimStrategy` to arbitrate.

- Responsible for: poll cadence, batch size, staging fetched candidates into
  the claim table.
- Not responsible for: where the backlog is stored (`TaskQueue`, split out
  above) or claim arbitration itself (`ClaimStrategy`).

**Candidate implementations**: `PollingSchedulingStrategy` (default, matches
task-execution.md: one feeder thread per node, batch of 500, pulled from
`TaskQueue`). No event-driven implementation planned for v1 — flagged as a
possible future implementation, not a current requirement.

**Open**: cluster-wide concurrency cap (Q-CAP) — does this interface need a
cap-aware variant, or does per-node pool sizing remain the only throttle?

---

## HeartbeatStrategy — [open]

Liveness/hang detection, currently under active discussion (Q-HEARTBEAT).

- Responsible for: signaling whether an in-flight task is still alive.
- Relationship to `ClaimStrategy`: originally designed as independent
  (decoupled cadence); palmyra76/workflow's unified-call pattern challenges
  that. Whether this ends up as a genuinely separate interface or folds into
  `ClaimStrategy.renew()` is the open question itself.

**No candidate implementations committed yet** — pending Q-HEARTBEAT resolution.

---

## PauseController — [draft]

Cluster-wide pause signal and propagation.

- Responsible for: a pause flag (set/read), propagation to nodes not
  currently holding a claim.
- Not responsible for: per-task checkpoint logic — `TaskContext.isPauseRequested()`
  is what a `TaskProcessor` actually polls.

**Candidate implementations**: `taskmesh-postgres` (DB flag row + poll,
matches task-execution.md: 15s poll on non-owning nodes, in-memory flag on
the owning node). `taskmesh-testkit` in-memory fake for tests.

---

## ProcessorRegistry — [draft]

Maps `task_type` → `TaskProcessor`. Owns registration-time and claim-time
validation.

- Responsible for: startup-time duplicate-registration rejection (loud
  failure, matches both peer projects' proven pattern); claim-time
  unregistered-type rejection (loud, per D7 — the confirmed gap neither
  peer project fully closed).
- Not responsible for: task_type versioning (see Q-ROLLING-DEPLOY — likely a
  distinct future concern, not folded into this interface yet).

**Candidate implementations**: `DefaultProcessorRegistry` in `-engine` —
framework-agnostic, built from a plain `List<TaskProcessor>` passed to its
constructor (duplicate `taskType()` across two entries throws at construction
time, mirrors nm_cleanser/workflow's proven registry pattern). No Spring
dependency in core. `taskmesh-spring-boot-starter` supplies the
`List<TaskProcessor>` via `@Component` auto-discovery for Spring Boot
consumers — same registry class either way, just a different way of
building the input list. `taskmesh-testkit` manual-registration fake for tests.

---

## HistoryWriter — [draft]

Per-attempt execution history — audit-only, never read by control flow
(explicit rule, borrowed from workflow's design note).

- Responsible for: recording attempt-level events (started, completed,
  failed, retried) at a configurable verbosity level.
- Not responsible for: influencing retry/claim decisions — those read task
  state directly, never history.

**Candidate implementations**: `taskmesh-postgres` (table-backed).
Levels (`NULL`/`MINIMAL`/`STANDARD`/`FULL`, default `STANDARD`) borrowed
directly from workflow's `HistoryWriter` design — matches this org's
Kafka-durable-audit-log goal, where a level knob trades granularity for
write volume at very large scale.

**Open**: does a `taskmesh-kafka` implementation belong in this project, or
is that consumer-specific (the originating toolkit's own requirement, not a
general taskmesh concern)? Leaning: interface stays generic, a Kafka-backed
implementation is a reasonable future taskmesh module given how common the
need likely is, but not required for v1.

---

## AuditEventSink — [draft]

Operator-initiated action log — separate from `HistoryWriter` because it's
low-volume, human-attributed, and never sampled.

- Responsible for: recording meaningful actions (pause/resume/retry/skip)
  with an attributed actor.
- Not responsible for: routine automatic state transitions — those are
  derivable from task state + `HistoryWriter`, not logged here (matches
  both peer projects' "meaningful actions only" filter).

**Candidate implementations**: `taskmesh-postgres`.

**Open**: actor-tagging shape — workflow's `APPLICATION_USER`/`SYSTEM`/
`ENGINE_MODULE` split is a good starting taxonomy; adopt as-is or adjust?

---

## IncidentStore — [draft]

Queryable "what's currently broken" — orthogonal to task state, not a
terminal-state flag (see the Failure & retry → Item-level failure handling
row in the comparison: none of the three peer designs uses a true
partial-failure terminal state; incidents are the better pattern).

- Responsible for: one row per incident, typed reason code (from
  `FailureClassifier`), open/resolved state.
- Not responsible for: retry scheduling.

**Candidate implementations**: `taskmesh-postgres`.

**Open**: minimum viable schema — `task_id, task_type, reason_code, message,
created_at, resolved_at` is the working draft; needs review once
`FailureClassifier`'s reason-code enum is settled.

---

## PayloadCodec — [open]

Isolates payload serialization and (future) schema versioning behind one seam.

- Responsible for: encode/decode between a `TaskProcessor`'s typed input and
  the engine's opaque stored payload.
- Not responsible for: actual schema migration logic — deferred per Q-PAYLOAD-VERSION.

**No candidate implementations committed** — depends on resolving whether a
`schema_version` field is added now (cheap insurance) even if unused in v1.

---

## TimeoutPolicy — [open]

Per-`task_type` execution timeout, enforced by `-engine`.

- Responsible for: a configurable wall-clock limit per task_type.
- Not responsible for: what happens after timeout — that's a `ClaimStrategy`
  / retry concern (does timeout trigger release-and-requeue, or something else?).

**No candidate implementations committed** — none of the three peer designs
has a working reference for "configurable per-type, actually enforced";
workflow's unbuilt config surface (`claim-timeout-seconds` etc.) is a
blueprint to borrow, not an implementation to copy.

---

## MetricsReporter — [open]

Per-task_type operational metrics.

- Responsible for: exposing counters/timers (throughput, latency, error rate)
  per task_type.
- Fresh design territory — neither peer project has a working metrics layer
  to draw from (see Q-OBSERVABILITY).

**Candidate implementations**: `taskmesh-metrics-micrometer` (real),
a no-op default living in `-api` or `-engine` so the module stays optional.

---

## Not yet interfaces (deliberately)

- **Idempotency** — a documented contract on `TaskProcessor`, not a pluggable
  behavior, and not an engine-owned ledger (a `CompletionLedger`-style
  interface was considered and rejected — see decisions.md D11: only the
  processor's domain logic can define "this sub-item is done," and an
  engine-level ledger would be a redundant, driftable second source of truth
  next to whatever existence/dedup check the processor already needs for
  correctness). The engine's only contribution is the stable `task_id` (D4).
  A CI-time idempotency contract test in `taskmesh-testkit` is worth
  building (run twice, assert no duplicate side effects) but isn't a
  taskmesh-core interface.
- **Logging/correlation (MDC)** — cross-cutting, populated once at the claim
  boundary inside `-engine`; not something a processor author configures or a
  consumer swaps out.
- **Processor code versioning** — explicitly out of scope, not deferred (see
  decisions.md D10). A consumer/deployment concern; taskmesh's core will
  never gain a version-aware `ProcessorRegistry` variant for this.
- **Transaction management** — considered and rejected as an interface
  (`TransactionRunner`) — see decisions.md D15. Core `-engine` never manages
  a processor's data on its behalf (no generic chunk-read/chunk-write
  machinery, unlike nm_cleanser's base classes), so there's nothing for the
  engine to broker. A `TaskProcessor` author uses whatever transactional
  tooling is already in their own classpath (`TransactionTemplate`, a plain
  `DataSource`, JPA — taskmesh doesn't care). **Documented guideline**: since
  `-engine` calls `process()` via a plain method call, never a Spring proxy,
  `@Transactional` on `process()` itself (or any method the engine calls
  directly) is silently ignored — demarcate transactions explicitly inside
  your own processor code instead of relying on annotation-based AOP at that
  boundary.
