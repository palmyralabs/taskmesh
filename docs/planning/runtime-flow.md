# Runtime flow — end-to-end walkthrough

Purpose: validate that every interface defined so far actually composes into
a coherent lifecycle, with no gaps at the seams. Written phase by phase,
naming only interface-level calls — no SQL, no schema, no table names. Any
concrete storage/query detail belongs in the implementation docs
(`claim-strategy.md`, `infra-implementations.md`) and is cross-referenced,
never repeated here. Gaps found while writing this are called out inline and
resolved or logged as open questions — that's the point of doing this pass
before drilling into individual implementations further.

**Structural reference**: the phase breakdown below is modeled on
palmyra76/workflow's actual multi-node runner — its poller/feeder
(`WorkflowPoller`/`JobPoller`), claim service (`ProcessClaimService`),
per-node heartbeat tick (`JobHeartbeat`, renews claim + liveness together),
and single failure-classification chokepoint (`FailureClassifier`) — since
it's the most complete real reference available for how these pieces
actually compose in a running multi-node system. Where taskmesh's design
deviates from workflow's, that's a deliberate call (see `decisions.md`), not
an oversight.

## Phase 0 — Node startup

1. `ProcessorRegistry` built from the set of registered `TaskProcessor`s.
   Duplicate `taskType()` ⇒ throws, refuses to boot (D7).
2. `ClaimStrategy` performs its own startup validation (implementation-
   specific — see `claim-strategy.md`).
3. Config resolved — profile defaults (`profiles.md`) + any overrides
   (`configuration.md`).
4. `PauseController` initial state read, in case this node is starting while
   the cluster is already paused.
5. `SchedulingStrategy`'s feeder starts.

## Phase 1 — Discovery

1. Feeder asks for claimable candidates — either directly from its own
   backing store, or via `TaskQueue` if that optional indirection is in use
   (D13). Exact query/predicate is implementation-specific; see
   `claim-strategy.md`/`infra-implementations.md`.
2. Fetched candidates are staged for claiming — mechanism is
   `ClaimStrategy`-implementation-specific, not part of this interface-level
   walkthrough.
3. Feeder attempts a claim per candidate (Phase 2), respecting
   `SchedulingStrategy`'s batch/pool/blocking-submit semantics.

## Phase 2 — Claim

1. `ClaimStrategy.claim(taskRef)` → `WON` or `LOST`.
2. `LOST`: silent, drop, move to the next candidate — normal, not logged
   above debug.
3. `WON`: proceed to Phase 3.

## Phase 3 — Pre-dispatch

1. `ProcessorRegistry.lookup(taskType)`. **Missing processor** (D7 — loud,
   never silent): immediately call `ClaimStrategy.release()` with a terminal
   disposition, write an `IncidentStore` entry (`reasonCode=NO_PROCESSOR`).
   **Resolved here**: this does **not** burn a retry attempt — it's a
   node/deployment misconfiguration, not a task failure, same principle as
   "a crash doesn't burn a retry" (D6). A *different* node that does have
   the processor registered will pick this task up once the claim is released.
2. `PayloadCodec.decode(rawPayload)` → typed input for the processor.
3. `TimeoutPolicy` resolved for this `taskType` → execution deadline set.
4. `TaskContext` constructed: task identity, decoded payload, an ownership
   handle wired to this claim, checkpoint hooks wired to `HeartbeatStrategy`
   + `PauseController`.
5. Submitted to the worker pool.

## Phase 4 — Execution

`TaskProcessor.onStart(ctx)` fires first, in its own short transaction, before
any work is done (D34). It is the only signal that says "this task is alive
again" after an operator rerun — the D33 pair identifies which kind of start it
is (`retryCount == 0` with `attemptCount > 1` is a rerun). A throw here aborts
the execution before `process()` runs and travels Phase 5b like any other
classified failure.

Then `TaskProcessor.process(ctx)` runs. At each checkpoint the processor calls
(granularity is the processor's own choice — per-document, per-chunk,
whatever fits):

- **Renewal** (`HeartbeatStrategy`): if `UnifiedRenewalHeartbeatStrategy` and
  past 50% of lease elapsed, calls `ClaimStrategy.renew()`. `LOST` ⇒
  `ctx.isOwnershipValid()` flips false; processor is expected to stop at its
  next safe opportunity (best-effort — see Phase 6, this can't be forced).
- **Pause check** (`PauseController`): `ctx.isPauseRequested()` — if true,
  processor should wind down and return early (best-effort, same caveat).
- **Progress** (`MetricsReporter`, optional): counters/timers updated.

**Gap found writing this**: `TimeoutPolicy` enforcement has no clean
mechanism specified. A wall-clock deadline is easy to *check* between
checkpoints, but forcibly reclaiming a thread that's blocked inside a single
long-running call (the same shape as the checkpoint-frequency gap surfaced
during design review) is a real Java limitation — thread interruption
doesn't preempt blocking I/O reliably. Practical answer for now:
`TimeoutPolicy` is enforced the same way as checkpoint frequency — as a
*detection* mechanism (flag an incident once a task exceeds its declared
timeout), not a *preemption* mechanism. Added to `Q-TIMEOUT` in
`open-questions.md`.

## Phase 5a — Success

1. `process()` returns normally.
2. `ClaimStrategy.release(COMPLETED)` — the engine opens the transaction (D34;
   `ClaimStrategy` opens none of its own) and, still inside it, calls
   `TaskProcessor.onTerminal(COMPLETED)`. A throw there rolls the release back
   and the task comes round again.
3. `HistoryWriter` records a `COMPLETED` event.
4. `MetricsReporter` increments completion counter, records duration.
5. `AuditEventSink` — **not** invoked; routine completion isn't a "meaningful
   operator action" (the filter both peer projects use).

`onTerminal` fires the same way from the other three terminal paths: a
`FAILED_TERMINAL` release (Phase 5c), a `RETRY` release whose budget ran out,
and D33's claim-time exhaustion in Phase 2. It never fires on a within-budget
retry, a yield, an ordinary reclaim, or a release that lost ownership.

## Phase 5b — Retryable failure

1. `process()` throws.
2. `FailureClassifier.classify(exception)` → `RETRYABLE` (default) + `reasonCode`.
3. `ClaimStrategy.release(RETRY, reasonCode)` — *(D33 renamed `REQUEUE` to
   `RETRY`; `YIELD` was added for cooperative hand-back, which consumes no
   budget)* — the exhaustion check (has
   this task used up its retry budget?) happens **inside** this call, not in
   `FailureClassifier` or `RetryPolicy` independently (D16 — full reasoning
   and the concrete mechanism live in `claim-strategy.md`). If the budget is
   exhausted, the disposition is escalated to `FAILED_TERMINAL` (Phase 5c)
   instead of `RETRY`. Otherwise `RetryPolicy` supplies the next retry
   time as part of the same atomic operation.
4. `HistoryWriter` records `ATTEMPT_FAILED` / `RETRIED`.
5. `MetricsReporter` increments retry counter.
6. `IncidentStore` — **not** written for a retry within budget; only on
   exhaustion or an outright non-retryable classification (Phase 5c).

## Phase 5c — Non-retryable or exhausted failure

Reached either directly (`FailureClassifier` says `NON_RETRYABLE`) or via
Phase 5b's exhaustion check.

1. `ClaimStrategy.release(FAILED_TERMINAL, reasonCode)`.
2. `IncidentStore` entry created (open incident).
3. `HistoryWriter` records `FAILED`.
4. `MetricsReporter` increments terminal-failure counter.
5. `AuditEventSink` — still not automatic; `IncidentStore` is the ops-facing
   signal for this, not the audit log (audit log stays human-action-only).

## Phase 6 — Lost ownership mid-execution

1. A `renew()` call returns `LOST` (Phase 4).
2. `ctx.isOwnershipValid()` flips false. Processor *should* stop at its next
   checkpoint — can't be forced (documented guideline, not enforced — see
   the checkpoint-frequency discussion in `decisions.md`/design review).
3. Engine does **not** call `release()` — there's nothing to release,
   another node already owns the claim.
4. **Addition found writing this**: even though no claim-state write
   happens, `HistoryWriter` should still record an `OWNERSHIP_LOST` event —
   it's audit-only and decoupled from control flow (the existing rule), so
   there's no reason to skip it just because this path skips the state
   transition. Gives ops a trail for "this node lost a task mid-flight"
   without needing to infer it from absence of other events.

## Phase 7 — Pause / shutdown mid-execution

1. `PauseController` flag observed at a checkpoint (Phase 4).
2. Processor winds down, returns early (best-effort).
3. Engine does **not** call `release()` — deliberate, matches
   `claim-strategy.md`'s "no explicit abandon call" rule. The claim expires
   naturally on its own timing.
4. Same `HistoryWriter` addition as Phase 6 — a `PAUSED` event, audit-only,
   no state-store write.

## Phase 8 — Recovery (steady state, no new mechanism)

Feeder's normal Phase 1 discovery naturally picks up anything abandoned in
Phase 6 or Phase 7 — visibility-based recovery, no sweeper (D3), nothing new
needed here.

---

## D16 — Retry-exhaustion check lives inside `ClaimStrategy.release()`, not `FailureClassifier`/`RetryPolicy`

`FailureClassifier` answers "is this kind of failure retryable in
principle?" — a property of the exception, doesn't need the attempt
counter. Whether *this specific attempt* has exhausted its budget is a
different question, and can only be answered atomically at the same point
the attempt counter is read/updated — which is inside `release()`'s own
atomic operation (the same reasoning that drove the earlier correction to
`release()`'s design — see `claim-strategy.md`). Folding the exhaustion
check in there, rather than making `RetryPolicy` or `FailureClassifier`
responsible for it independently, avoids a second, separately-timed read of
mutable state for the same value.

**Concrete mechanism (query predicates, locking, column names) lives in
`claim-strategy.md`** — not repeated here.
