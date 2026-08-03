# Decisions

Resolved during the originating grill session. Each entry: the decision, the
reasoning, and what it rules out.

## D1 — This is a refinement of task-execution.md, generalized into a reusable engine

Not a parallel design, not a domain-specific rewrite. The multi-node
policy-migration task-execution design (HPE/LIC toolkit, `task-execution.md`)
is the concrete starting point; taskmesh extracts its domain-agnostic core.

**Rules out**: designing taskmesh's claim/lease model from scratch independent
of what's already been devil's-advocate reviewed in the source design.

## D2 — Fully generic task abstraction, not policy-aware-but-reusable

The engine core knows `task_id` / `task_type` / opaque payload and a pluggable
`TaskProcessor`. It has no concept of "policy" or "folder" anywhere in the core.

**Rules out**: a "policy-shaped" engine that a second consumer subclasses or
configures around. Domain concepts live entirely in consumer-supplied
`TaskProcessor` implementations.

## D3 — Shared claim/staging table, not per-node local staging

One shared table (matching task-execution.md's `in_flight`), visible to every
node. A node's feeder stages fetched work into it; any node's feeder can see
and claim any staged/expired row.

**Why**: cross-node visibility is what makes "a dead node's abandoned work is
naturally re-claimed by a live node" free — no sweeper thread needed. A
per-node local table would reintroduce exactly the "dead node hoards work"
failure mode the source design deliberately avoided.

**Rules out**: per-node local staging tables, and any recovery mechanism that
depends on one.

## D4 — Task identity: `(task_type, task_id)`, task_id supplied by the caller

No synthetic/surrogate ID layer. The consumer passes its own natural key
(e.g. `policyNumber`) as `task_id`; the engine's only requirement is
`(task_type, task_id)` uniqueness.

**Why**: matches the source design's spirit (the policy number *is* the unit,
no need to wrap it), keeps idempotent re-submission simple (re-submitting the
same `task_id` for a `task_type` is naturally an upsert), avoids UUID
indirection for callers who already have a natural key.

**Rules out**: an engine-minted UUID `task_id` with the natural key relegated
to the payload.

## D5 — HeartbeatStrategy ships as two implementations, not one forced answer

Resolved by not resolving to a single design: `HeartbeatStrategy` gets both
`ExplicitHeartbeatStrategy` (nm_cleanser-style, separate ≤30s heartbeat,
active hang detection) and `UnifiedRenewalHeartbeatStrategy` (workflow-style,
folded into lease renewal at lease/2, no separate watchdog). See
`infra-implementations.md` for the full spec of both.

**Default recommendation**: `UnifiedRenewalHeartbeatStrategy` for v1 (matches
the most battle-tested multi-node peer design); `ExplicitHeartbeatStrategy`
available per-task_type where fast hang detection genuinely matters (e.g.
long silent external-API calls).

## D6 — Retryable vs. non-retryable failure classification is a confirmed gap to close

Both nm_cleanser and palmyra76/workflow independently built a two-way
retryable/non-retryable distinction (nm_cleanser: `ItemProcessingException` vs
`UnrecoverableException`; workflow: default-retryable + `NonRetryableException`
carrying a typed `IncidentType`). The source task-execution.md design does not
have this — every failure requeues-with-backoff regardless of cause.

**Decision**: taskmesh adopts the distinction via a `FailureClassifier`
interface, single chokepoint, typed reason code (not free-text).

## D7 — Unregistered task_type must fail loudly, not silently

Both nm_cleanser (missing DB registration row) and workflow's `ElementProcessor`
path fail silently on an unregistered type. workflow's `ServiceTaskHandler`
path does it right — throws a terminal, incident-worthy exception.

**Decision**: `ProcessorRegistry` validates completeness (every task_type ever
queued has a registered processor) at claim time at minimum, ideally with an
earlier startup-time check too. No silent skip, ever.

## D8 — Module shape: `-api` (interfaces) + `-engine` (driving impl) as the core, everything else is a pluggable implementation module

See `module-layout.md`. Consumer-supplied `TaskProcessor` implementations
(e.g. policy migration) live in the *consuming* project, not in taskmesh.

## D9 — Name: taskmesh

Chosen over `shuttle` / `convoy` for being self-explanatory from the module
list alone (`taskmesh-api`, `taskmesh-engine`, `taskmesh-postgres`) without
needing a metaphor explained to new engineers.

## D10 — Processor code versioning / rolling deploy safety is out of scope

Not deferred, not a future interface — explicitly out of scope for taskmesh.
Rolling-deploy safety for a given `task_type`'s code is a consumer/deployment
concern, not an engine concern.

**Why**: the engine has no way to reason about what changed in a processor's
logic between versions — only the consumer knows whether a code change is
safe to apply to already-queued/in-flight tasks. Neither peer project solved
this either (see the comparison sheet) — there's no proven pattern to adopt,
and forcing an interface into existence for it would be speculative design.

**Rules out**: a `ProcessorVersionResolver`-type interface, task_type version
tags, or any claim-time version-compatibility check in taskmesh's core.
Consumers needing this must build it themselves (e.g. versioned task_type
strings like `POLICY_MIGRATE_V2`, at the consumer's discretion).

## D11 — Idempotency stays entirely a TaskProcessor-owned contract; no engine-level completion ledger

Considered and rejected an opt-in `CompletionLedger` interface (engine tracks
sub-item completion, e.g. per-document, and skips already-marked items on
replay).

**Why rejected**: only the processor's domain logic can meaningfully define
"this sub-item is done" (e.g. task-execution.md's real pattern — check
whether a document already exists with a matching hash in the target system
before re-uploading it). An engine-owned ledger would be a second, redundant
source of truth sitting next to that check, and the two could drift (ledger
says done, target system disagrees after a partial write).

**What the engine actually provides**: a stable `task_id` (D4), identical
across every retry — the one piece of idempotency support that's genuinely
the engine's to give, since only the engine knows a retry is happening.
Everything else (existence checks, dedup logic, safe-to-retry sub-work) is
`TaskProcessor` guideline material, documented, not enforced by an interface.

**CI-time backstop (not yet committed, worth revisiting)**: a `taskmesh-testkit`
idempotency contract test — run a `TaskProcessor` twice against a spy
dependency with the same `task_id`, fail the test if the second run produces
any additional side-effecting call. Catches idempotency bugs pre-cluster;
doesn't require an engine-level interface.

**Rules out**: any `CompletionLedger`/sub-item-tracking interface in
taskmesh's core.

## D12 — Implementation variants are organized by workload profile, not by origin project

Replaces the earlier "cleanser-style vs. workflow-style" framing. The origin
project a variant was inspired by was never the actual point — workload
shape (duration × concurrency) is. Three profiles currently recognized:
`BATCH` (nm_cleanser: long-running, low-concurrency), `HIGH_VOLUME`
(task-execution.md — **this project's own origin consumer**: medium-running,
high-concurrency), `REACTIVE` (palmyra76/workflow: short-running,
low-latency). Registry, not a closed enum — more profiles addable later.

**Why this surfaced**: `SchedulingStrategy`'s "workflow-style" variant had
silently absorbed both the `REACTIVE` and `HIGH_VOLUME` profiles into one
implementation with one set of tuning numbers, even though task-execution.md
(this project's actual consumer) and palmyra76/workflow sit at genuinely
different points on the duration/concurrency spectrum. See `profiles.md` for
the full per-interface variant-selection matrix.

**Packaging**: one thin aggregator module per profile
(`taskmesh-profile-batch`/`-highvolume`/`-reactive`) — config/defaults only,
not a fork of implementation code. See `module-layout.md`.

**Rules out**: choosing implementations by "which peer project it resembles"
as the primary lens going forward; profile is the lens, peer project is just
where a given implementation's mechanics originally came from.

## D13 — TaskQueue is a new, optional interface — not required by any default implementation

Splits "where the backlog is stored" out of `SchedulingStrategy` (which
previously conflated storage with polling policy). Added specifically
because `HIGH_VOLUME`'s backlog depth (tens of millions of rows, see D12/
`profiles.md`) is the one scenario where "swap the backlog store" might
someday be a real need — Postgres partial-index scanning at that depth is
untested, not known-insufficient.

**Explicitly optional**: every default `SchedulingStrategy` implementation
(`BATCH` and `HIGH_VOLUME` alike) queries `tm_task` directly, with no
`TaskQueue` indirection. `PostgresTaskQueue` is the only implementation
specified, and it's the same query already in `claim-strategy.md` given an
interface name — nothing changes for any current consumer. `TaskQueue` is a
seam for a future need, not infrastructure every implementation must adopt.

**Rules out**: making `TaskQueue` a mandatory layer between `SchedulingStrategy`
and the database for any profile; requiring a second (non-Postgres)
implementation before this is usable — none exists yet, none is required to.

## D14 — `taskmesh-engine` is framework-agnostic; Spring Boot is an optional adapter, not a core dependency

Resolves Q-TXBOUNDARY. Core `taskmesh-engine` (feeder, claim loop, dispatcher,
worker pools) has zero Spring/Spring Boot dependency — plain
`ExecutorService`-based Java, matching `taskmesh-api`'s existing "no
framework dependency" rule. A new optional module, `taskmesh-spring-boot-starter`,
depends on `-engine` (not the reverse) and supplies Spring Boot conveniences
for consumers who want them: `@ConfigurationProperties` binding (per
`configuration.md`'s dual-path design), `@Component`-based `ProcessorRegistry`
population, and a `TransactionTemplate`-backed `TransactionRunner`.

**Why**: this org's peer projects are Spring Boot apps, but taskmesh itself
is meant to be embedded, not to *be* an application — forcing Spring Boot on
every consumer (including `taskmesh-testkit`, which must work with zero
framework at all) was an unexamined default, not a deliberate choice, and
contradicted `taskmesh-api`'s own rule.

**Direct consequence — resolves Q-TXBOUNDARY**: since `-engine` invokes
`TaskProcessor.process()` via a plain method call (no Spring proxy possible
in a framework-agnostic core), `@Transactional` on a processor method would
be silently ignored — nm_cleanser's exact documented gotcha. This is now a
**documented guideline for processor authors**, not an interface — see D15;
on reflection, `TransactionRunner` wasn't the right fix for it.

**Rules out**: any core interface or `-engine` internals assuming a Spring
context exists; Spring-only construction paths for `ProcessorRegistry` or
config classes (every config class must also work via a plain builder, per
`configuration.md`).

## D15 — No `TransactionRunner` interface; transaction management is entirely the TaskProcessor's own concern

D14 initially concluded `TransactionRunner` was load-bearing, reasoning from
nm_cleanser's gotcha. On reflection that reasoning didn't hold: nm_cleanser
needed `TransactionTemplate` because *its own base classes* do generic
chunk-read/chunk-write on the framework side — the framework needed a
transaction mechanism because the framework was the one doing the work.
taskmesh's `-engine` never touches a processor's data; `TaskProcessor.process(ctx)`
is one opaque call, engine has no generic chunk-processing machinery running
on a processor's behalf. There is nothing for an engine-owned interface to
broker — a processor author already has full access to whatever
transactional tooling exists in their own classpath, zero taskmesh
involvement required. This is the same shape as D11 (idempotency): don't
build engine machinery for something purely domain-specific that the engine
can't meaningfully broker anyway.

**What replaces it**: a documented guideline, not an interface — since
`-engine` calls `process()` via a plain method call, never a Spring proxy,
`@Transactional` on `process()` (or any engine-invoked method) is silently
ignored; processor authors must demarcate transactions explicitly inside
their own code.

**Rules out**: a `TransactionRunner` interface in `taskmesh-api`;
`JdbcTransactionRunner`/`SpringTransactionRunner` implementations (both
removed — nothing to implement).

## D16 — Retry-exhaustion check lives inside `ClaimStrategy.release()`, not `FailureClassifier`/`RetryPolicy`

Full reasoning and the corrected `release()` SQL live in `runtime-flow.md`
(found while walking the end-to-end flow) and `claim-strategy.md`. Summary:
`FailureClassifier` decides retryable-in-principle (no `attempt_count`
needed); whether *this* attempt has exhausted its budget can only be
answered atomically where `attempt_count` is read under lock — inside
`release()`'s fenced transaction, same reasoning as the Attack-1 fix.

## D17 — `nodeId` removed from `ClaimStrategy`/`HeartbeatStrategy` method signatures; bound internally by the implementation instead

`ClaimStrategy.claim/renew/release` and `HeartbeatStrategy.onCheckpoint` no
longer take a `nodeId` parameter. Surfaced while starting `-engine` design:
a given `ClaimStrategy`/`HeartbeatStrategy` instance is constructed once per
node, at Phase 0 startup (`runtime-flow.md`), and lives for that node's
entire process lifetime — the `nodeId` value passed on every call was always
identical for a given instance. Threading it through every call site would
have meant `-engine` carrying node identity from startup config down into
every claim/renew/release invocation in the dispatch loop, for no behavioral
benefit (ownership fencing is unaffected — the implementation still fences
on the same value, just read from its own constructor-bound field instead of
a call argument).

**Rules out**: any `-engine` code passing `nodeId` explicitly per call;
implementations must bind their own node identity at construction (e.g. via
a constructor parameter or a `NodeIdProvider` collaborator).

**Consequence for `taskmesh-testkit`**: `InMemoryClaimStrategy` (see
`claim-strategy.md`'s Testkit section) must be constructed one instance per
simulated node, sharing a common backing store, to unit-test multi-node
claim races — rather than one instance queried with different `nodeId`
arguments. Considered more realistic (mirrors actual deployment topology)
than a loss.

## D18 — `Candidate(TaskRef, byte[] rawPayload)` replaces bare `TaskRef` in `CandidateHandler`/`TaskQueue`

Surfaced while implementing `-engine`'s dispatch loop: nothing in the
existing interfaces carried the raw payload from discovery through to the
point `PayloadCodec.decode()` needs it. `ClaimStrategy.claim()` returns only
a `ClaimOutcome` enum, `TaskRef` is identity-only, and `TaskQueue.fetchClaimable`
returned bare `TaskRef`s too — there was no method anywhere that returned
payload bytes for a given task. Since `SchedulingStrategy` (or `TaskQueue`,
when used) already reads the payload column while querying `tm_task` for
claimable rows (D13), it already has the bytes in hand at exactly the point
it hands candidates to the engine — the natural, lowest-blast-radius fix is
to carry them through in that same handoff rather than inventing a separate
payload-lookup interface.

**Change**: new record `Candidate(TaskRef taskRef, byte[] rawPayload)`.
`CandidateHandler.onCandidates(String taskType, List<Candidate> candidates)`
and `TaskQueue.fetchClaimable(...)` both return/carry `Candidate` instead of
bare `TaskRef`. `ClaimStrategy`, `HistoryWriter`, `AuditEventSink`, etc. are
unaffected — they only ever needed identity, not payload.

**Rules out**: a separate `PayloadStore`/`fetchPayload(TaskRef)` interface —
would have duplicated a read `SchedulingStrategy`/`TaskQueue` implementations
already do.

## D19 — `ReleaseOutcome` gains a third value, `EXHAUSTED`

Surfaced alongside D18 while writing `-engine`'s failure-handling path: D16
put the retry-exhaustion check *inside* `ClaimStrategy.release()`, atomic
with the `attempt_count` read — meaning a `REQUEUE` request can silently
become a terminal failure inside that one call. But `ReleaseOutcome` only
had `OK`/`LOST_OWNERSHIP` — the engine had no way to tell, from the return
value, whether its `REQUEUE` was honored (in-budget retry) or escalated
(exhausted). Both looked like `OK`. That distinction is load-bearing for the
engine's own bookkeeping: an in-budget retry must not open an `IncidentStore`
entry or call `MetricsReporter.taskFailed`; an exhausted one must do both
instead of `taskRetried`.

**Change**: `ReleaseOutcome` is now `{ OK, LOST_OWNERSHIP, EXHAUSTED }`.
`EXHAUSTED` is returned only when the caller's disposition was `REQUEUE` and
the implementation escalated it internally; a `COMPLETED`/`FAILED_TERMINAL`
release still only ever returns `OK` or `LOST_OWNERSHIP`.

**Rules out**: engine code that treats `OK` as "my requested disposition was
honored as given" for the `REQUEUE` case — it must branch on `EXHAUSTED`
before assuming a plain retry happened.

## D20 — First persistence implementation module is `taskmesh-jpa` (Spring Data JPA, vendor-portable), not `taskmesh-postgres`

`module-layout.md` originally named this module `taskmesh-postgres`, on the
assumption of Postgres-native SQL and Postgres-specific tuning (the HIGH_VOLUME
hot/cold table split's autovacuum/fillfactor settings, specifically). Changed
while starting the module: build it as a portable JPA implementation instead —
`taskmesh-jpa`, using Spring Data JPA repositories (`@Modifying @Query` JPQL,
not native SQL) so it works against any Hibernate-supported database, not
just Postgres.

**Consequence for schema design**: since the module is deliberately not
Postgres-specific, the hot/cold table split that HIGH_VOLUME's Postgres
tuning calls for (profiles.md) doesn't apply here — `taskmesh-jpa` uses one
unified `tm_task` table. A future Postgres-specific implementation (if the
HIGH_VOLUME profile's scale genuinely demands hot/cold separation) would be
a *separate* module, not a variant of this one.

**Consequence for D14's Spring-agnostic boundary**: `taskmesh-jpa` depends on
Spring Data JPA directly (chosen over plain JPA/EntityManager) — accepted as
a deliberate exception. D14's "framework-agnostic" rule was scoped to
`taskmesh-engine`/`taskmesh-api` specifically (the `-engine` internals and
core interfaces), not to every infra implementation module; `taskmesh-jpa`
is a pluggable implementation a consumer chooses to depend on, exactly like
`taskmesh-postgres` was always going to be. `-engine` itself still has zero
Spring dependency.

**Design patterns pulled from `palmyra76/workflow` (prior art, not a
dependency)**: atomic `UPDATE ... WHERE <eligibility predicate>` for
claim/renew with row-count as the race signal (no `SELECT FOR UPDATE` needed
there); `SELECT ... FOR UPDATE` reserved specifically for D16's
exhaustion-check-and-mutate path, which genuinely needs a read-then-write
within one transaction; explicit `TransactionTemplate` per method rather than
`@Transactional` proxying, so correctness doesn't depend on the caller
running inside a Spring-proxied bean. Deliberately *not* pulled: workflow's
three-window (reclaim/affinity/timeout) claim eligibility — its own git
history documents a real bug (the "60s–150s dead zone") caused by mixing a
short write-minimization window into the eligibility predicate; taskmesh's
single-window design (already the documented lean in `claim-strategy.md`)
sidesteps that whole bug class by construction.

## D21 — `Candidate` payload access becomes a lazy, cached, `TaskRef`-keyed provider

Refines D18. Originally `Candidate` carried `byte[] rawPayload` eagerly
fetched at discovery time, decoded once by `-engine` right after a claim was
won. Two problems surfaced while building `taskmesh-jpa`: (1) discovery
batches can be large (task-execution.md: batch size 500) but only the
candidates that actually win their claim ever need their payload — eager
per-row fetch/decode wastes I/O on every candidate that loses the race; (2)
a per-candidate closure capturing one `TaskRef` is wasteful when discovery
already knows how to look up any row's payload generically.

**Change**: `PayloadProvider` (new `@FunctionalInterface`,
`Object load(TaskRef taskRef) throws Exception`) replaces raw bytes in
`Candidate`: `Candidate(TaskRef taskRef, PayloadProvider payloadProvider)`.
Taking `TaskRef` as a parameter (rather than closing over it) lets discovery
hand out *one shared provider instance* for an entire batch instead of
allocating a closure per row. `TmTaskRepository.findClaimable` now projects
a lightweight `ClaimableRow` (identity only, no payload column); the payload
is fetched via a second, targeted query only inside the provider, only for
the row whose claim is actually won.

**Further change — payload load moves into `TaskContext` itself, lazily,
with caching**: rather than `-engine` eagerly calling
`payloadProvider.load(taskRef)` right after a claim is won,
`DefaultTaskContext.payload()` now calls it itself on first access and
caches the result (a single `TaskContext` is only ever driven by one worker
thread, so no synchronization is needed around the cache). Consequences:
- A `TaskProcessor` that never reads `ctx.payload()` never pays for the
  fetch/decode at all.
- A decode failure is no longer a special-cased "pre-dispatch" branch in
  `-engine` — it surfaces as a normal exception thrown from inside
  `TaskProcessor.process()` (wrapped as `PayloadLoadException`, since
  `TaskContext.payload()` can't declare a checked exception), routed through
  the same `FailureClassifier`/retry path as any other processing failure.
  This ate `-engine`'s `handleFailure`/`openTerminalIncident` `preDispatch`
  parameter entirely — removed as dead code.
- `-engine` no longer holds or requires a `PayloadCodec` at all — decoding is
  now entirely a discovery-implementation-internal concern (wrapped inside
  whatever `PayloadProvider` `SchedulingStrategy`/`TaskQueue` hands out).
  `TaskMeshEngine.Builder` no longer has a `payloadCodec()` method or
  validates one.

**Rules out**: `Candidate` carrying raw bytes directly; `-engine` calling
`PayloadCodec`/`PayloadProvider` itself outside of `TaskContext.payload()`;
any future engine code assuming payload is already resolved before
`TaskProcessor.process()` is invoked.

## D22 — `TaskLedger` replaces `SchedulingStrategy` + `TaskQueue`; the engine owns scheduling, not the implementation

Surfaced while designing engine-level `enqueue`/`execute` entry points (D25):
`SchedulingStrategy` (self-scheduled, `start(CandidateHandler)`/`stop()`) and
`TaskQueue` (D13's "optional, storage-only" split, `fetchClaimable`/`enqueue`)
had drifted into covering the same ground from two directions, and every
`SchedulingStrategy` implementation was duplicating near-identical
`ScheduledExecutorService` boilerplate for no benefit — the engine already
owns `SchedulingConfig` (poll interval, etc.), so it should own the timer
too.

**Change**: both retired, replaced by one interface:
```java
public interface TaskLedger {
    List<TaskRef> pullPending(String taskType, Instant now, int limit);
    void enqueue(TaskRef taskRef);
}
```
Control inverts: the **engine** calls `pullPending` on its own schedule
(from `SchedulingConfig.queryIntervalSeconds`), passing `limit` as exactly
how many slots are actually free in the execution queue's `regularArray`
(D25) — implementations never self-schedule, never over-fetch, and shrink
to pure data-access classes. `enqueue` is the one write path both the
engine's public `enqueue()` and `execute()` methods persist through (D25) —
they differ only in what the engine does *after* persisting, not in how the
row gets written.

**Naming**: "TaskLedger," not "TaskQueue" or "TaskBacklog" — "queue" is
reserved exclusively for the in-memory `FifoPriorityQueueImpl` (D25) to
avoid the persisted/in-memory confusion that name previously caused;
"backlog" was considered and rejected as less accurate once the "app
populates this table directly, ledger's read side just works" use case
(D22 point (c) discussion) was factored in — "ledger" better captures "the
durable record of what work exists and its state," including rows nothing
in taskmesh ever wrote.

**Two implementations planned** (`taskmesh-profile-highvolume`): `SimpleTaskLedger`
(single-node) and `MultiNodeTaskLedger` (D27's 3-window claim model) — see D27.

**Rules out**: any `SchedulingStrategy`/`TaskQueue`-shaped type in `-api`
going forward; per-implementation self-scheduling; a `CandidateHandler`-style
push callback (superseded by D24's plain `List<TaskRef>` return value).

## D23 — `PayloadProviderFactory` (keyed by task_type) replaces a single shared `PayloadProvider`; `PayloadStore` write-side kept but unused

Two payload-related decisions made together while removing payload
parameters from `enqueue`/`execute` (D25):

1. **`PayloadProviderFactory`**: `enqueue(TaskRef)`/`execute(TaskRef)` no
   longer take a payload argument at all — `PayloadProvider.load(TaskRef)`
   (D21) is relied on to fetch it, keyed purely by identity. But a single
   engine-held `PayloadProvider` instance would be wrong for a multi-task_type
   deployment (e.g. HIGH_VOLUME's `POLICY_MIGRATE`/`VERIFY` split, same
   scenario `ScopedPauseController` exists for) where different task types
   may have entirely different payload sources. Fix: `PayloadProviderFactory.getProvider(String taskType)
   -> PayloadProvider`, one factory instance held by the engine, looked up
   per `taskRef.taskType()` when building `DefaultTaskContext`.
2. **`PayloadStore extends PayloadProvider`** (adds `store(TaskRef, Object payload)`)
   is kept in `-api` as a forward-looking extension point even though no
   current entry point calls it — `enqueue`/`execute` dropping their payload
   parameters means taskmesh never writes payload data anywhere in the
   locked design. Kept anyway on the reasoning that it may be needed later;
   revisit removal if it stays uncalled.

**Rules out**: any engine code holding a single un-keyed `PayloadProvider`;
`enqueue`/`execute` accepting a payload argument (superseded — see D25).

## D24 — `Candidate` and `CandidateHandler` retired

Consequence of D22 + D23. `Candidate` (D18/D21) existed to carry a
`PayloadProvider` alongside a `TaskRef` because discovery implementations
could each construct their own provider per batch. With `PayloadProviderFactory`
supplying providers keyed by `task_type` (D23) rather than discovery
constructing one per call, there is nothing left for `Candidate` to bundle —
discovery now just returns `List<TaskRef>` (`TaskLedger.pullPending`, D22).
`CandidateHandler`'s decoupling *purpose* (keep discovery ignorant of
claim/dispatch internals) still matters, but with `pullPending` being a
plain return value from an engine-initiated call (not a self-scheduled push),
there's no callback type left to name — the engine just receives the list
directly.

**Rules out**: any `-api` type carrying a payload provider alongside a
`TaskRef` for discovery purposes; a named push-callback interface for
discovery → engine handoff.

## D25 — Three work-submission entry points (`enqueue`/`submit`/`execute`) over a two-array priority execution queue

The engine gains real producer-facing API, not just internal dispatch of
already-discovered work. Grilled from prior-art checks across `workflow`,
nm_cleanser, and the user's own `crownshaft` (`FifoQueueImpl.isHalfFull()`/
`SimpleTaskRouter.canEnqueue()` — validated precedent for the half-capacity
gate specifically, from `ksvraja`'s own prior code).

**Execution queue** (memory-only, per the earlier `TaskQueue`-naming
clarification — never persisted, never pluggable): `FifoPriorityQueueImpl`,
two internal arrays. `priorityArray` — fixed capacity **5** (a constant for
now, promotable to a config property later). `regularArray` — the rest of
total capacity. Pinned directly as the `BlockingQueue<Runnable>` a
`ThreadPoolExecutor` is constructed with, so priority-first draining lives
entirely inside the queue's own `take()`/`poll()` — Java's `ThreadPoolExecutor`
already calls that whenever a worker goes idle, no separate engine-side
ordering logic needed. Replaces `BlockingSubmitExecutor`'s plain
`ArrayBlockingQueue`.

**Three entry points**:

| | Persists (`TaskLedger.enqueue`) | Pushes to execution queue | Admission |
|---|---|---|---|
| `enqueue(TaskRef)` | Yes | Into `regularArray`, if admitted | Gated by `canEnqueue()`-style check (`regularArray.isHalfFull()`, mirroring `crownshaft`'s `SimpleTaskRouter` exactly); skips the push (persist-only) if the gate fails — the row is still durable, `submit` picks it up later |
| `submit(List<TaskRef>)` (poller-internal, driven by `TaskLedger.pullPending`, D22) | No — reads existing rows only | Into `regularArray` | Unconditional, no gate |
| `execute(TaskRef)` | Yes | Into `priorityArray`, always drained first | If `priorityArray` is full: reject the *immediate push* after a configured timeout — the row is already durably persisted either way, so nothing is lost; `submit` remains the safety net |

Both `payload` and `taskType` params dropped from `enqueue`/`execute` — see
D23.

**Rules out**: a single undifferentiated push path for all producers (the
nm_cleanser/`workflow` default, explicitly rejected here since taskmesh
wants true dequeue-order priority, not just admission fairness); `execute`
blocking indefinitely on a full `priorityArray` (workflow's own
drop-and-rely-on-poller philosophy informed the timeout-then-reject choice
over blocking).

## D26 — In-memory dedup guard across `enqueue`/`submit`/`execute`

A `TaskRef`'s row stays whatever non-terminal status it's in (`QUEUED`/`RELEASED`)
right up until a worker thread actually calls `claim()` on it — so between
"pulled/pushed into the execution queue" and "actually claimed," a second
producer (a poller cycle, a concurrent `execute()` call) could re-discover
and re-push the same `TaskRef`, wasting a worker-pool slot and a full claim
round-trip on a guaranteed-`LOST` duplicate.

Cross-checked against `workflow`, nm_cleanser, and `crownshaft`: nm_cleanser
and `crownshaft` have no such guard and don't need one — both have exactly
**one** producer path into their dispatch pipeline (their own poll cycles
are non-overlapping by construction — nm_cleanser via a manual
`AtomicBoolean`-guarded self-reschedule loop, `crownshaft` via a single
dedicated thread — so a producer can't race itself, and there's no second
producer to race against). `workflow` is the one with a real guard
(`InFlightSet`, `ConcurrentHashMap`-backed, `add`/`remove`/`contains`,
verified leak-proof on every realistic failure path including uncaught
`Error` since removal is in a `finally`) specifically because it has
**multiple** producers (interactive path + two pollers) racing each other.
taskmesh's `enqueue`/`submit`/`execute` is architecturally the same
multi-producer shape as `workflow`, not the single-producer shape of the
other two — `InFlightSet` is the validated precedent to adopt, not
optional.

**Change**: engine-held `Set<TaskRef>` (or equivalent), consulted by all
three of `enqueue`/`submit`/`execute` before pushing into either array of
`FifoPriorityQueueImpl`; removed in a `finally` around actual claim/dequeue,
mirroring `InFlightSet`'s add-gates/finally-removes shape exactly.

## D27 — Multi-node `ClaimStrategy` adopts the 3-window (reclaim/affinity/timeout) model; amends D20's "deliberately not pulled" call; new `RELEASED` status

**Reverses part of D20.** D20 deliberately did not adopt `workflow`'s
3-window claim eligibility, reasoning that taskmesh's single-window design
sidesteps the "60s–150s dead zone" bug `workflow`'s own git history
documents. On reflection (prompted by re-examining why nm_cleanser/`crownshaft`
never needed this at all — see D26): those two never solved true multi-node
claim arbitration in the first place, only single-producer/single-instance
dispatch. **For a genuinely multi-node `ClaimStrategy`, the 3-window model
is not optional complexity to defer — it's what makes cross-node ownership
hand-off (a released task preferring its last owner for a while, before
opening to any node) actually work.** The single-window design avoided the
dead-zone bug by not having the feature the bug was in, not by solving the
same problem more simply.

**The dead-zone bug's actual lesson carries forward unweakened**: `reclaimWindow`
(short, write-minimization only) must never appear in the eligibility
`WHERE` — only inside the `SET ... CASE` timestamp-refresh. Eligibility
keys on `affinityWindow`/`claimTimeoutCutoff` only. Adopting the 3-window
*model* while keeping this specific discipline avoids re-introducing the
bug D20 was protecting against.

**Schema change**: `TaskStatus` gains `RELEASED` (`QUEUED, CLAIMED, RELEASED,
COMPLETED, FAILED`) — distinguishes "never claimed" from "claimed then
given back for retry" so affinity has something to key on; `node_id` is no
longer nulled on a retryable release (kept as "last owner" metadata). A
retry-requeue (`release()`'s `REQUEUE` branch) now sets `status='RELEASED'`
instead of `'QUEUED'`.

**`claim()`'s `WHERE`, translated from `workflow`'s fixed SQL onto this schema**:
```sql
UPDATE tm_task SET status='CLAIMED', node_id=:me,
  claimed_at = CASE WHEN node_id <> :me OR claimed_at IS NULL OR claimed_at < :reclaimCutoff
                     THEN :now ELSE claimed_at END, ...
WHERE task_type=:type AND task_id=:id AND (
     status = 'QUEUED'
  OR (status = 'CLAIMED'  AND node_id = :me)
  OR (status = 'RELEASED' AND node_id = :me AND updated_at >= :affinityCutoff)
  OR (status = 'RELEASED' AND (updated_at IS NULL OR updated_at < :affinityCutoff))
  OR (status = 'CLAIMED'  AND updated_at < :claimTimeoutCutoff)
)
```

**Two `ClaimStrategy`-family implementations, both in `taskmesh-profile-highvolume`,
both implementing `TaskLedger`**: `SimpleTaskLedger` (single-node — no
lease, no affinity/reclaim/timeout at all, `node_id`-only optimistic guard,
startup-recovery requeues this node's own stale `CLAIMED` rows) and
`MultiNodeTaskLedger` (the 3-window model above). `TaskLedger.pullPending`'s
discovery query must mirror whichever `ClaimStrategy` variant's eligibility
predicate exactly, per `workflow`'s own documented lesson ("the poller must
share the exact same eligibility predicate as claim, or it drifts") —
already the reasoning behind the crash-recovery fix found earlier this
session.

**Rules out**: D20's blanket "3-window model deliberately not pulled"
framing as a permanent stance — it was correct for a single-window design
that hadn't yet confronted real multi-node affinity needs, not a closed
question. `reclaimWindow` appearing anywhere in an eligibility `WHERE`
clause, in any implementation, ever.

## D28 — `tm_task` splits into cold (`tm_task`) + hot (`tm_in_flight`) tables; amends D20's single-table rationale

**Amends D20 again.** D20's `TmTaskEntity` Javadoc argued the hot/cold split
(the toolkit's original `migration_task`/`in_flight` design) was Postgres-specific
autovacuum/fillfactor tuning that "doesn't apply to a portable JPA
implementation anyway." That framing conflated two different things: the
*tuning* (fillfactor, `autovacuum_vacuum_scale_factor`) genuinely is
Postgres-specific and stays a DDL-level, per-deployment concern — but the
*structural* win (claim/renew/release churn never touching the same table as
permanent COMPLETED/FAILED history) doesn't require native SQL at all. JPQL
supports cross-entity joins and `EXISTS` subqueries against unrelated
entities (JPA 2.1+), so the split is fully portable. Adopted per explicit
instruction, prompted by revisiting the toolkit's original design while
building `emgr-taskmesh`.

**Split**:
- `tm_task` (cold) — `TaskStatus` narrows to `QUEUED, COMPLETED, FAILED` here
  only. One row per task for its entire lifetime; touched at enqueue and on
  terminal/requeue transitions. Attempt count, `next_retry_at`, `reason_code`,
  `error_summary` all live here.
- `tm_in_flight` (hot, new) — `TaskStatus` narrows to `CLAIMED, RELEASED` (or
  `NULL` = unclaimed) here only. `node_id`, `claimed_at`, `lease_expires_at`
  live here. Created alongside the `tm_task` row at enqueue time (`status =
  NULL`, eager, not lazily staged on first claim attempt like the toolkit's
  original design) and **deleted entirely on any terminal outcome**
  (`COMPLETED`, `FAILED`, including exhaustion) — per explicit instruction,
  so the hot table's steady-state size stays bounded by current
  backlog+active count, never by all-time history.

**Discovery is driven FROM the hot table**, per explicit instruction —
`findClaimableMultiNode`/`findClaimableSingleNode` now `SELECT ... FROM
TmInFlightEntity f JOIN TmTaskEntity t ON ...`, not the other way round. The
claim UPDATE mirrors this (`UPDATE TmInFlightEntity f ... WHERE ... AND
EXISTS (SELECT 1 FROM TmTaskEntity t WHERE ... AND t.status = QUEUED)`) —
must stay in sync with the discovery query's predicate, the same
"eligibility drift" lesson as D27, now spanning two tables instead of one.

**Release, split by outcome**:
- Terminal (`COMPLETED`/`FAILED`) — delete the hot row first (fenced on
  `node_id`+`status='CLAIMED'`; 0 rows deleted = `LOST_OWNERSHIP`, cold table
  never touched), then update the cold row unconditionally by PK. Ownership
  fencing moved entirely to the hot-table op; the cold `terminalUpdate` no
  longer needs a `node_id` predicate.
- Requeue (retryable) — `MultiNodeTaskLedger` sets the hot row to `RELEASED`
  (node_id retained, same affinity purpose as D27) via a fenced UPDATE; on
  exhaustion the hot row is deleted immediately after (terminal, the
  just-set `RELEASED` no longer means anything). `SimpleTaskLedger` resets
  the hot row straight to unclaimed (`status=NULL, node_id=NULL`) instead —
  no affinity concept there, matching its existing "never `RELEASED`" rule.

**Bug found while implementing this** (pre-existing, not introduced by the
split): `enqueue()`'s `findById`-then-`save` on both tables is not atomic.
Two genuinely concurrent callers for the same brand-new `TaskRef` (two
nodes, or two threads on one node) can both pass the empty check before
either row exists, then collide on the primary key — `InFlightSet` doesn't
protect this path since `enqueue()`/`execute()` call `TaskLedger.enqueue`
*before* that in-memory gate. Fixed by catching `DataIntegrityViolationException`
and treating it as a benign lost race — enqueue is documented as idempotent,
and the other caller's row is exactly the outcome this call wanted too.

**Rules out**: "one unified table is simpler, so it's the natural choice for
a portable implementation" as a closed question — it conflated tuning
(genuinely non-portable) with structure (portable via JPQL). Any future
`TaskLedger` implementation that reintroduces claim churn into the same
table as terminal history without discussing the tradeoff explicitly.

## D29 — `taskmesh-profile-highvolume` becomes `taskmesh-impl`; single module for all three profiles, selected by implementation choice, not tunables

**Supersedes module-layout.md's three-`taskmesh-profile-*`-modules plan.**
Comparing what actually differs across BATCH/HIGH_VOLUME/REACTIVE in
profiles.md's matrix: most of it is tunable `Duration`/`int` values already
passed as constructor args to the *same* classes (`MultiNodeTaskLedger`'s
lease/reclaim/affinity/claimTimeout, `SchedulingConfig`'s pool/batch/poll
settings) — not different code. Three separate Gradle modules for that would
mostly be empty wrapper shells around a shared implementation module that
was never actually built (`taskmesh-jpa`/`taskmesh-postgres`, per D20). Per
explicit instruction: one module, all three profiles; **the numeric tunables
are not part of what defines a profile** — they stay ordinary consumer-side
config, documented as suggested defaults, never baked into taskmesh itself.
A profile is defined by *which implementation* a consuming app's own wiring
config selects per `-api` interface — taskmesh only supplies the building
blocks, per `infra-implementations.md`'s original two-variant-per-interface
spec (cleanser-style vs. workflow-style):

| Interface | Variant A | Variant B | Profile mapping |
|---|---|---|---|
| `ClaimStrategy`/`TaskLedger` | `SimpleTaskLedger` (single-node) | `MultiNodeTaskLedger` (3-window) | BATCH → A (or B w/ long lease); HIGH_VOLUME/REACTIVE → B |
| `HeartbeatStrategy` | `ExplicitHeartbeatStrategy` (new — active hang detection) | `JpaHeartbeatStrategy` (checkpoint-triggered) | BATCH → A; HIGH_VOLUME/REACTIVE → B |
| `HistoryWriter` | `AttemptHistoryWriter` (new — one row/attempt) | `JpaHistoryWriter` + `HistoryLevel` (new enum: `MINIMAL`/`STANDARD`/`VERBOSE`) | BATCH → A; HIGH_VOLUME → B@VERBOSE; REACTIVE → B@MINIMAL |
| `PauseController` | — | `JpaPauseController` (cluster-wide only) | all three — **no per-task_type variant**, reversed below |

**`ExplicitHeartbeatStrategy` built without new engine hooks.** The obvious
design (start a background renewal timer on claim, stop on release) needs
lifecycle hooks the engine doesn't expose (`onClaimed`/`onReleased` don't
exist). Built instead as a *reactive* check inside the existing
`onCheckpoint` touchpoint: tracks the gap since the previous checkpoint call
per `TaskRef`, opens an incident if it exceeds a threshold, still delegates
to `ClaimStrategy.renew()` either way. Detection lags by up to one
checkpoint interval versus a true background timer — accepted tradeoff for
avoiding engine surgery for a profile with no current real consumer.

**`ScopedPauseController` (per-task_type pause) built, then fully reverted**
per explicit instruction — pause stays cluster-wide only across all three
profiles. `PauseController` gained, then lost again, `isPauseRequested(String)`/
`requestPause(String)`/`resume(String)` default-method overloads and a
matching `TaskMeshEngine.pause(String taskType, String)`/`resume(...)` pair;
`DefaultTaskContext` briefly called the scoped overload. All reverted to the
original three-method, cluster-wide-only shape. Recorded here so the
apparent gap against profiles.md's original HIGH_VOLUME `PauseController`
row isn't mistaken for an oversight later.

**Rename + package split**: `taskmesh-profile-highvolume` → `taskmesh-impl`
(module directory and Gradle coordinate); package
`com.palmyralabs.taskmesh.profile.highvolume` → `com.palmyralabs.taskmesh.impl`,
with `@Entity`/JPA-ID classes moved to `.entity` and `Repository` interfaces
to `.repository`, leaving only the profile-selectable implementation classes
(`MultiNodeTaskLedger`, `SimpleTaskLedger`, `Jpa*`, `AttemptHistoryWriter`,
`ExplicitHeartbeatStrategy`, `HistoryLevel`, `ExponentialBackoffRetryPolicy`)
at the package root. **Sharp edge hit during the move**: `TmTaskRepository`/
`TmInFlightRepository`'s `@Query` JPQL strings embed the enum's *fully-qualified
class name as a string literal* (e.g. `com.palmyralabs.taskmesh.profile.highvolume.TaskStatus.QUEUED`)
— invisible to ordinary import-based refactoring, since it's annotation text,
not a compiled reference. Any future package move must grep `@Query` string
bodies specifically, not just rely on import statements resolving cleanly.

**Rules out**: three `taskmesh-profile-*` sub-projects as the near-term
packaging shape. Revisit only if a profile's dependencies genuinely diverge
(a profile needing Kafka, say) — not before.

## D30 — `TaskMeshEngine` gets a runtime pool-resize primitive; latency tracking and the resize *policy* stay entirely consumer-side

The migration toolkit (HPE/LIC) wants backpressure-aware throttling: when
source-query or target-upload latency degrades, shrink the migration
worker pool; when it recovers, grow it back. Two ways to split this:
(a) taskmesh owns a `LatencyTracker` abstraction and a backpressure
controller that calls its own resize internally, or (b) taskmesh only
exposes the mechanical capability (resize the pool) and the consumer
owns measuring latency and deciding when/how much to resize.

**Chose (b), per explicit instruction.** Latency is a domain signal (what
counts as "source" vs "target" latency, what op to measure, PoC-rolling-average
vs production-percentile) that has nothing to do with task orchestration —
taskmesh stays domain-agnostic (per D2) even for its own operational tuning.
`TaskMeshEngine` gains exactly two methods: `adjustPoolSize(int)` (bounded to
`[1, the pool size the engine was built with]` — never scales past the
configured ceiling) and `currentPoolSize()`. No `LatencyTracker` interface,
no percentile/rolling-average implementations, no scheduled backpressure
controller — those all belong in the consuming app (`emgr-*` in the
migration toolkit repo), built against this resize primitive.

**Rules out**: a `taskmesh-backpressure` module, a `MetricsReporter`
extension for latency, or any taskmesh-side threshold/hysteresis config.
If a second consumer later wants the same backpressure controller logic,
that argues for extracting a *shared consumer-side library*, not moving it
into taskmesh — taskmesh's job stops at "resize the pool safely."

## D31 — `taskmesh-impl` split into three fully-independent tiers (`-simple`/`-pro`/`-max`); `BacklogPromoter` decouples `tm_task` enqueue from `tm_in_flight` hot-row creation

A ~100 crore (1 billion) row one-shot migration surfaced a real limit in the
D28 hot/cold split: `tm_task` and `tm_in_flight` are created **eagerly as a
pair** at `enqueue()`. If a migration enqueues its full backlog up front,
`tm_in_flight` — the hot table `findClaimableMultiNode` scans continuously —
balloons to the same size as the entire backlog, not the "current
backlog+active" window it's designed for.

**`BacklogPromoter` (new `-api` interface)**: `onEnqueue(TaskRef, nodeId)` —
called synchronously right after a task's `tm_task` row is written — and
`promote(taskType, now, limit)` — called periodically by the engine on its
own `ScheduledExecutorService` (`TaskMeshEngineImpl.backlogScheduler`, same
`scheduleWithFixedDelay` idiom as the existing claim poller). Two
implementations: `NoopBacklogPromoter` (default everywhere — `onEnqueue`
creates the hot row immediately, `promote` is a true no-op, reproducing
D28's original eager-pair behavior exactly) and `JpaBacklogPromoter`
(taskmesh-max only — `onEnqueue` no-ops, `promote` creates hot rows for the
oldest still-`promoted_at IS NULL` backlog rows in bounded batches, stamping
`tm_task.promoted_at` so a repeat cycle doesn't re-promote). This is a
**deliberate, documented amendment to D28** for consumers using
`JpaBacklogPromoter`: the pair is no longer created atomically at enqueue.

**Module split**: rather than bolt scale-specific machinery onto one
`taskmesh-impl`, persistence became three tiered, **fully independent**
artifacts — each with its own JPA entities/repositories/DDL, chosen
deliberately over a shared-core module so a schema change in the largest
tier (partitioning, widened PKs) can never leak into the smallest tier's
code or DDL:

- **`taskmesh-simple`** (renamed from `taskmesh-impl`) — small-scale,
  single-node by default, `MultiNodeTaskLedger` available. Always
  `NoopBacklogPromoter`; `enqueue()` unchanged from D28 (still creates both
  rows inline).
- **`taskmesh-pro`** — medium/large-scale, `MultiNodeTaskLedger` by default,
  switchable to single-node. Structurally near-identical to `-simple` today
  (accepted duplication cost of the independence guarantee) — differs mainly
  in default wiring; still always `NoopBacklogPromoter`.
- **`taskmesh-max`** — the one tier whose `enqueue()` only ever writes
  `tm_task`, deferring hot-row creation entirely to whichever
  `BacklogPromoter` is wired (`NoopBacklogPromoter` still the default;
  `JpaBacklogPromoter` opt-in). Also the intended home for the `partitionKey`/
  `PartitionedTaskLedger` work discussed alongside this — **not yet
  implemented**, tracked separately.

**`taskmesh-spring-boot-starter` dependency direction inverted.** It used to
own `TaskmeshEngineAutoConfiguration`/`ProfileFactories`/`TaskmeshJpaAutoConfiguration`
and depend on `taskmesh-impl`. With three independent tiers, the starter
can't depend on all three (defeats the independence point) or reference a
single tier's classes (arbitrarily privileges one tier). Fix: the starter
now holds only tier-agnostic pieces (`TaskmeshProperties`, `WorkloadProfile`,
`Slf4jMetricsReporter`, `SingleProviderPayloadProviderFactory`) with **zero**
dependency on any tier; each tier depends on the starter instead and ships
its own `<tier>.starter.{ProfileFactories,TaskmeshJpaAutoConfiguration,
TaskmeshEngineAutoConfiguration,TaskmeshProfileBuilder}` plus its own
`AutoConfiguration.imports`. No `@ConditionalOnClass` tier-detection needed —
a consumer depends on exactly one tier module, so only that tier's
`AutoConfiguration.imports` is ever on the classpath. `Jitter` (previously
nested in `ExponentialBackoffRetryPolicy`, itself duplicated per tier) moved
to `taskmesh-api` as a standalone enum, since `TaskmeshProperties` needed one
tier-agnostic reference to it.

**Rules out**: a shared `taskmesh-impl-core` module for entities/repositories
(considered, explicitly rejected in favor of full independence); folding
`WorkloadProfile` or the metrics-backend Variant-A/B extension point into
more tiers (both stay orthogonal axes, not additional tiers).

## D32 — `taskmesh-max`: caller-supplied `partitionKey`, `PartitionedTaskLedger` replaces `MultiNodeTaskLedger`

Resolved through an extended grill-me/devil's-advocate session (not a single
top-down design). Key decisions:

- The partition key is **caller-supplied**, never computed/hashed by
  taskmesh. `TaskRef` gains a third component, `partitionKey` (nullable,
  2-arg constructor overload defaults it to `null`) — kept on the **shared**
  `taskmesh-api` record rather than a `taskmesh-max`-only subtype, because
  the engine's "never branches on tier" invariant means `TaskMeshEngineImpl`
  can't `instanceof`-check a tier-specific `TaskRef` when constructing
  `TaskContext`/`HistoryEvent`. `taskmesh-simple`/`-pro` are entirely
  unaffected — the 2-arg constructor keeps every existing call site
  compiling.
- `taskmesh-max` ships **only** `PartitionedTaskLedger` (renamed from
  `MultiNodeTaskLedger`, not a class added alongside it) — `SimpleTaskLedger`
  is **dropped entirely** from this tier. Partitioning is a multi-node/scale
  concept; a single-node deployment has no claim race to partition around,
  and this tier's whole reason to exist is large-scale (~100 crore row)
  migrations, not a single-node workload.
- `enqueue()`/`rerun()` **hard-fail** (`IllegalArgumentException`) on a
  missing `partitionKey` — consistent with the repo's "silence is a bug"
  invariant. No default/fallback bucket, and `rerun()` specifically has no
  fallback cross-partition lookup either — the operator-facing caller always
  supplies it, or the call throws.
- `partition_key` is added to `tm_task`, `tm_in_flight`, `tm_history_event`,
  `tm_incident`, `tm_attempt_history`. It's part of the **primary key** on
  four of those five (Postgres requires the partition column in every unique
  constraint on a table that's physically partitioned) — the exception is
  `tm_in_flight`, a plain column only, since that table is never physically
  partitioned (the `JpaBacklogPromoter`-throttled feeder keeps it bounded
  regardless, D31). `tm_audit_event` is **explicitly out of scope** —
  deferred, mainly because of its nullable-`task_id` cluster-wide-event case
  (`PAUSE_REQUESTED`/`RESUME_REQUESTED` have no task, so no natural partition
  key either).
- **Caller contract, not enforced by the ledger**: `(taskType, taskId)` must
  map to exactly one, stable `partitionKey`. `tm_task`'s widened PK
  technically admits two different `partitionKey` values for the same
  `(taskType, taskId)`, but `tm_in_flight`'s PK does not include
  `partitionKey` — a caller that violates this contract gets an unsupported,
  inconsistent hot-row state (the second enqueue's hot-row creation silently
  no-ops), not a clean error. Documented on `PartitionedTaskLedger`'s class
  Javadoc; not covered by a test — an out-of-contract scenario, not a
  supported one.
- taskmesh ships **zero partitioning opinion** — no default `HASH`/`LIST`
  scheme, no modulus, no `PARTITION BY` clause in the reference DDL at all.
  Only the column + widened key. The actual partitioning scheme is entirely
  the consuming application's own DDL, same as every other Postgres-specific
  tuning note already in `docs/schema.md` (fillfactor, autovacuum).
- **Real Hibernate limitation hit during implementation**: `IDENTITY`
  generation isn't supported inside an `@IdClass` composite key
  ("Identity generation isn't supported for composite ids"). `tm_history_event`/
  `tm_incident` (both previously a simple surrogate `@Id @GeneratedValue(IDENTITY)`)
  switch to an explicit `@SequenceGenerator`/`SEQUENCE` at this tier — which
  also happens to be the right mechanism for keeping `id` globally unique
  across physical partitions anyway (an `IDENTITY`/`SERIAL` column isn't
  reliably shared across Postgres partitions; an explicit sequence is).
- `HistoryEvent`/`Incident` already embed `TaskRef`, so `HistoryWriter`/
  `IncidentStore` `-api` interfaces needed **no signature change** —
  `partitionKey` reaches the `Jpa*` implementations for free through the
  `TaskRef` they're already handed.

**Rules out**: hashing/deriving the partition key internally (rejected in
Q1 of the grill-me session — the calling application always supplies it);
a taskmesh-shipped default `PARTITION BY` scheme (rejected — zero opinion,
consumer-owned); keeping `SimpleTaskLedger` in `taskmesh-max` as a switchable
option (rejected — not this tier's use case); a fallback cross-partition
lookup in `rerun()` (rejected — hard-fail instead, matching `enqueue()`).
