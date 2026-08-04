# ClaimStrategy — detailed spec

Full contract for the interface introduced in `interfaces.md` and summarized
in `infra-implementations.md`. This is the load-bearing interface — every
other infra piece (`SchedulingStrategy`, `HeartbeatStrategy`, `PauseController`)
either calls into it or assumes its guarantees.

## Division of responsibility with SchedulingStrategy

`ClaimStrategy` does **not** decide which tasks to attempt claiming or when —
that's `SchedulingStrategy`'s job (fetch/stage candidates, batch size, poll
cadence). `ClaimStrategy` is purely the atomic per-task race-arbitration
primitive: given one `(task_type, task_id)`, decide a single winner. The
feeder calls `claim()` once per candidate it has already decided to attempt.

## Interface contract

```
interface ClaimStrategy {
    ClaimOutcome claim(TaskRef taskRef);
    RenewOutcome renew(TaskRef taskRef);
    ReleaseOutcome release(TaskRef taskRef, ReleaseRequest request);
}

record TaskRef(String taskType, String taskId)

enum ClaimOutcome   { WON, LOST }
enum RenewOutcome   { RENEWED, LOST }
enum ReleaseOutcome { OK, LOST_OWNERSHIP }

record ReleaseRequest(
    TerminalDisposition disposition,   // COMPLETED | RETRY | YIELD | FAILED_TERMINAL  (D33)
    String reasonCode,                 // from FailureClassifier; null on COMPLETED
    String errorSummary                // null on COMPLETED
)
```

Node identity (`nodeId`) is deliberately **not** a method parameter — a
`ClaimStrategy` implementation is constructed once per node and binds its
own node id internally (constructor param, `NodeIdProvider`, etc.). See D17
in `decisions.md`. SQL below still references `:me` — that placeholder now
comes from the implementation's own bound field, not a call argument.

**Deliberately not part of this interface**: classifying retryable vs.
terminal — that's `FailureClassifier`'s job, called by the engine *before*
`release()`, since it's a pure function of the exception (no shared mutable
state, safe to compute outside the transaction).

**Deliberately NOT precomputed and NOT part of `ReleaseRequest`**: backoff
timing (`nextRetryAt`). An earlier version of this spec had the engine
pre-compute `nextRetryAt` from `RetryPolicy` and pass it in. That's a bug:
`RetryPolicy` needs the current `attempt_count`, and the only safe place to
read it is inside the same fenced transaction that writes it — a read taken
beforehand can go stale if this node's claim already expired and another
node has since bumped `attempt_count` itself. Two nodes computing competing
backoff decisions off different generations of the same counter is exactly
the kind of bug the fencing design exists to prevent elsewhere.

**Fix, matching task-execution.md's actual pattern** (`attempt_count`
incremented as part of the same fenced write, not read-then-passed-in):
when `disposition == REQUEUE`, `ClaimStrategy.release()` invokes `RetryPolicy`
*itself*, inside the fenced transaction, using the `attempt_count` it reads
under that same transaction. `RetryPolicy` stays a separate, swappable
strategy — only *when* it's invoked moves, from "by the engine, before
calling release()" to "by ClaimStrategy, as a same-transaction collaborator."
`ClaimStrategy` takes a `RetryPolicy` as a constructor/wiring dependency for
this purpose; it still never makes the retryable-vs-terminal *decision*
itself, only applies the already-decided backoff formula atomically.

## Semantics

### `claim()`

Atomic, single winner. `LOST` is a normal, silent outcome — not an error, not
logged above debug level. The caller (feeder) just moves to the next candidate.

### `renew()`

Called on a cadence determined by `HeartbeatStrategy` (see infra-implementations.md
— `UnifiedRenewalHeartbeatStrategy` calls this directly as its heartbeat;
`ExplicitHeartbeatStrategy` calls it independently on its own schedule).
`LOST` means the claim expired or was stolen before this renewal landed —
the caller must stop all further work on this task immediately and not call
`release()` afterward (there's nothing to release; another node owns it now).

### `release()` — the terminal write, fenced by ownership

This is the single most important correctness property in the whole
interface: **the terminal write must be fenced by `node_id`, in the same
transaction as whatever state change it represents.**

- `OK`: this node still owned the claim at release time; the outcome was
  applied.
- `LOST_OWNERSHIP`: 0 rows affected — this node's ownership was already gone
  (lease expired and someone else claimed it, most likely because this node
  was slow/stalled past its own lease). **The caller logs an audit warning
  and does nothing else** — no compensating write, no retry of the release.
  Whatever the new owner does is authoritative; a stale node must not fight
  it (this is the TOCTOU case the whole fencing design exists to prevent).

### Pause/shutdown — no explicit abandon call

There is deliberately no `abandon()`/`release-without-outcome` method. On
pause or shutdown, a task simply **stops being renewed** and the claim
expires naturally on its own lease timer; visibility-based recovery (D3)
picks it back up after resume, on any node. Adding an explicit release-back-
to-QUEUED call here would just be a second code path doing what lease expiry
already does — more surface area for no benefit.

## Data model

### `LeaseClaimStrategy` (multi-node)

```
tm_task (cold — one row per task in the active set + history)
  task_type       varchar        \
  task_id         varchar        / composite PK
  status          varchar        -- DISCOVERED | QUEUED | COMPLETED | FAILED
                                  -- (IN_PROGRESS lives only in tm_claim, never here)
  attempt_count   int
  max_attempts    int
  next_retry_at   timestamptz
  reason_code     varchar        -- last FailureClassifier outcome, null if never failed
  error_summary   text
  created_at      timestamptz
  updated_at      timestamptz
  -- partial index on (status) WHERE status = 'QUEUED', for the feeder query

tm_claim (hot — only staged/active tasks; tiny, aggressive autovacuum)
  task_type          varchar    \
  task_id            varchar    / composite PK, FK-equivalent to tm_task
  node_id             varchar   NULL   -- NULL = staged, unclaimed
  claimed_at           timestamptz
  lease_expires_at     timestamptz
  staged_at            timestamptz
  -- staging INSERT is ON CONFLICT DO NOTHING — informational, not a reservation
  --   (matches task-execution.md's in_flight design exactly)
```

Claim SQL:
```sql
UPDATE tm_claim
SET node_id = :me, claimed_at = now(), lease_expires_at = now() + :lease
WHERE task_type = :type AND task_id = :id
  AND (node_id IS NULL OR lease_expires_at < now())
```
0 rows updated ⇒ `LOST`.

Renew SQL: same predicate, `WHERE ... AND node_id = :me AND lease_expires_at >= now()`
— renewal only succeeds while this node still owns an unexpired claim; a
renewal attempt after expiry (this node was too slow) correctly returns `LOST`.

Release SQL (fenced), one transaction:
1. `DELETE FROM tm_claim WHERE task_type=:type AND task_id=:id AND node_id=:me`
   — check this FIRST. 0 rows deleted ⇒ `LOST_OWNERSHIP`, roll back, do not
   touch `tm_task` at all.
2. Only if step 1 deleted a row (ownership confirmed): apply the `tm_task`
   terminal write.
   - `COMPLETED`/`FAILED_TERMINAL`: straightforward status update.
   - `REQUEUE`: `SELECT attempt_count, max_attempts FROM tm_task WHERE
     task_type=:type AND task_id=:id FOR UPDATE`. **Exhaustion check happens
     here** (D16, `runtime-flow.md`): if `attempt_count + 1 > max_attempts`,
     the disposition is overridden to `FAILED_TERMINAL` in this same
     transaction (falls through to the `FAILED_TERMINAL` case below) — an
     `IncidentStore` write follows, same as any other terminal failure.
     Otherwise, `RetryPolicy.nextRetryAt(attemptCount + 1)` computed in
     application code *inside this still-open transaction*, then `UPDATE
     tm_task SET attempt_count = attempt_count + 1, next_retry_at = :computed,
     status='QUEUED', reason_code=:reasonCode, error_summary=:errorSummary
     WHERE task_type=:type AND task_id=:id`. No separate read-then-write
     outside the transaction boundary — the `FOR UPDATE` and the `UPDATE`
     are both inside the one transaction that step 1's ownership check gates.

### `SingleNodeClaimStrategy` (single-node)

No separate hot table — claim state lives directly on `tm_task`:
```
tm_task
  ... (same cold columns as above, plus:)
  node_id   varchar NULL   -- set while status effectively IN_PROGRESS
```
`status` gains an implicit `IN_PROGRESS` meaning via `node_id IS NOT NULL`
rather than a hot/cold split — unnecessary complexity for a single node with
no cross-node race to arbitrate. Claim SQL: `UPDATE tm_task SET node_id=:me
WHERE task_type=:type AND task_id=:id AND status='QUEUED' AND node_id IS NULL`.
Recovery: on startup, `UPDATE tm_task SET node_id=NULL, status='QUEUED' WHERE
node_id = :myPriorInstanceId` (or simply: this node is the only writer, so any
row with `node_id IS NOT NULL` at startup is definitionally stale).

## Config

| Key | Applies to | Default | Constraint |
|---|---|---|---|
| `claim.lease.durationSeconds` | `LeaseClaimStrategy` | 300 | must satisfy the startup assertion below |
| `claim.renew.intervalSeconds` | `LeaseClaimStrategy` (via HeartbeatStrategy) | `lease/2` | — |

**Startup assertion (carried from task-execution.md, non-negotiable)**:
`lease/2 > socketTimeout + margin` — refuse to boot if config drift would
let a slow single operation blow through a renewal cycle and cause a false
lease expiry mid-task. `SingleNodeClaimStrategy` has no equivalent — no
lease, nothing to assert.

## Testkit implementation

`InMemoryClaimStrategy` (`taskmesh-testkit`) — same `LOST`/`WON`/`LOST_OWNERSHIP`
semantics as `LeaseClaimStrategy`, backed by a `ConcurrentHashMap` instead of
Postgres. Exists to unit-test `-engine`'s dispatch/renewal/release logic
itself (not `TaskProcessor` logic — that's what the separate `TaskContext`
fakes are for) without a database.

## Open item carried over

The richer 3-window claim-timing model (reclaim/affinity/timeout, from
palmyra76/workflow) is a possible future config shape for `LeaseClaimStrategy` —
not a new variant, just a richer `claim.lease.*` config surface. Deferred
until real multi-node contention data justifies the added complexity
(unchanged from the earlier open-questions discussion).
