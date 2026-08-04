# `tm_*` table reference

This document describes the seven tables owned by each of the three persistence
tiers — `taskmesh-simple`/`taskmesh-pro`/`taskmesh-max` (packages
`com.palmyralabs.taskmesh.simple`/`.pro`/`.max`, entities under `.entity`,
repositories under `.repository` — D29) — the JPA-backed persistence
implementation of taskmesh's `-api` contracts (`TaskLedger`, `ClaimStrategy`,
`HeartbeatStrategy`, `HistoryWriter`, `AuditEventSink`, `IncidentStore`,
`PauseController`, `BacklogPromoter`), each housing all three workload profiles
(BATCH/HIGH_VOLUME/REACTIVE) rather than just HIGH_VOLUME.

> **D31 — three independent tiers, not one `taskmesh-impl`.** Each tier owns
> its own copy of these tables (own entities, repositories, DDL) — deliberately
> not shared, so a tier-specific schema change (e.g. `taskmesh-max`'s
> `promoted_at`) can never leak into another tier.
>
> **D32 — `taskmesh-max` only: `partition_key`, caller-supplied and required.**
> `tm_task`, `tm_in_flight`, `tm_history_event`, `tm_incident`, and
> `tm_attempt_history` all gain a `partition_key` column at this tier —
> `PartitionedTaskLedger` (this tier's only `ClaimStrategy`/`TaskLedger`; there
> is no single-node variant here) throws on `enqueue()`/`rerun()` if it's
> missing, rather than defaulting to an unpartitioned bucket. It's part of the
> **primary key** on four of those five tables (Postgres requires the
> partition column in every unique constraint on a table you physically
> partition) — the one exception is `tm_in_flight`, where it's a plain column
> only, since that table is never physically partitioned (the feeder-throttled
> `JpaBacklogPromoter` design keeps it bounded regardless). `tm_audit_event` is
> explicitly **excluded** — deferred, revisited separately. taskmesh ships
> zero partitioning opinion (no `PARTITION BY`, no scheme, no modulus) — see
> the DDL notes below and `docs/planning/decisions.md`'s D32 entry.
>
> The table shapes below are otherwise identical across all three tiers.

Applications embedding taskmesh (via `MultiNodeTaskLedger` or
`SimpleTaskLedger`) never write to these tables directly — the engine and its
`Jpa*` implementations own all writes. Application code reads from them for
operational/reporting purposes (dashboards, REST status endpoints, ad-hoc
queries) and via the standard `TmTaskRepository`/etc. Spring Data repositories
if it needs custom queries beyond what taskmesh ships.

DDL for a given consuming application lives in that application's own
`dbscripts/` (Hibernate is run with `ddl-auto=validate`, never
auto-create/update) — see `dbscripts/edms_mig/007_tm_task.sql` through
`012_tm_control_flag.sql` in the migration toolkit for a worked example.

> **`tm_task` + `tm_in_flight` are a pair, not two independent tables** (D28):
> every enqueued task has exactly one row in each, from enqueue until a
> terminal outcome deletes the `tm_in_flight` row. See their sections below
> for the split rationale — read them together, not in isolation.

> **The hot `tm_in_flight` row is the sole source of truth for query/claim/release.**
> All the state those operations need — `status` (including the `QUEUED`
> reservation), `node_id`, the nested counters (`attempt_count`/`retry_count`,
> bounded by `max_retries`)
> and the backoff gate (`next_retry_at`) — lives on the hot row. `tm_task` is
> written only at enqueue and, as a cold snapshot, on the terminal transition;
> query/claim/release never read it. This is a deliberate tightening of D28:
> the discovery/claim queries no longer join out to `tm_task` at all (a hot
> row exists iff the task is non-terminal), and a retryable release touches
> only the hot row.

> **Postgres note**: the `TEXT`-like columns (`error_summary`, `detail`,
> `message`) are mapped with `@JdbcTypeCode(SqlTypes.LONGVARCHAR)`, not
> `@Lob`. On Postgres, `@Lob` on a `String` defaults to the `oid` large-object
> type, not `text` — a classic Hibernate 6 + Postgres mismatch. Define these
> columns as `TEXT` in DDL and keep the entities as `LONGVARCHAR`; H2 (used in
> taskmesh's own tests) doesn't have this pitfall since it has a native CLOB
> type, so this only surfaces against a real Postgres instance.

---

## `tm_task` (cold)

The ledger table — one row per task (`task_type` + `task_id`) for its
**entire lifetime**, touched only at enqueue and on the **terminal**
transition (a retryable requeue no longer touches it — that's now hot-row-only).
Deliberately carries **no payload column**: payload resolution is entirely the
consuming application's `PayloadProviderFactory`/`PayloadProvider` concern,
never taskmesh's to store.

`status` here only ever holds `QUEUED`, `COMPLETED`, or `FAILED` — `CLAIMED`/
`RELEASED` live exclusively on `tm_in_flight` below (D28). This split exists
so this table's autovacuum cost stays proportional to *write volume*
(enqueue + terminal transitions), not to claim/lease-renewal *frequency* — a
table that accumulates every `COMPLETED`/`FAILED` row forever must never also
be the target of high-frequency in-place UPDATEs, or autovacuum has to scan
an ever-growing table for churn confined to a small, currently-active
subset. Mirrors the migration toolkit's original `migration_task`/`in_flight`
design (see D28 for the full rationale and why it's portable JPA, not
Postgres-specific tuning).

| Column             | Notes                                                                                  |
|---------------------|-----------------------------------------------------------------------------------------|
| `task_type`, `task_id` (PK) | Task identity. `task_type` is a runtime string key (e.g. `"POLICY_MIGRATE"`); `task_id` is opaque to taskmesh — the consuming app decides its format (e.g. `branchCode:policyNumber`). |
| `status`            | `QUEUED` (at enqueue), then `COMPLETED` or `FAILED` at the terminal transition. Never read by claim/discovery. |
| `attempt_count` / `retry_count` / `max_retries` | **Terminal snapshot** — the hot row's counters (see below) copied verbatim on the terminal transition, unchanged. `0`/placeholder until then. Both counters are here deliberately: without `retry_count`, "how many retries did this burn before it failed" is unanswerable once the hot row is deleted, which is one of the signals the D33 split exists to expose. |
| `reason_code`, `error_summary` | Set on the terminal transition (`COMPLETED` clears them; `FAILED` records why). |
| `created_at`, `updated_at` | Standard audit timestamps. |

There is no `next_retry_at` here — the backoff gate moved to `tm_in_flight`
with the rest of the claim-driving state.

**`taskmesh-max` only**: an additional nullable `promoted_at` column, `NULL`
until `BacklogPromoter` creates the matching `tm_in_flight` row (immediately,
under the default `NoopBacklogPromoter`; in bounded batches, under
`JpaBacklogPromoter` — see D31). A partial index
(`idx_tm_task_unpromoted ON tm_task (created_at) WHERE promoted_at IS NULL
AND status = 'QUEUED'`) keeps `JpaBacklogPromoter`'s backlog scan cheap
regardless of how much promoted/terminal history accumulates.

## `tm_in_flight` (hot)

The claim-state table and the **sole source of truth for query/claim/release**
— all claim/renew/release churn lands here, never on `tm_task`. One row per
task that's been enqueued and isn't yet terminal: created **eagerly, alongside
the `tm_task` row, at enqueue time**, transitions through claim states as a
`ClaimStrategy` drives it, and is **deleted entirely** the moment the task
reaches a terminal outcome (`COMPLETED`, `FAILED`, including retry exhaustion)
— so this table's steady-state size tracks current backlog+active count, never
all-time history.

On the `enqueue`/`execute` path the row is created `status = QUEUED`,
**reserved to the enqueuing node** (`node_id` set); a bulk/external populate
path may instead insert an unreserved `status = NULL` row directly.

| Column             | Notes                                                                                  |
|---------------------|-----------------------------------------------------------------------------------------|
| `task_type`, `task_id` (PK) | Same identity as the paired `tm_task` row. |
| `status`            | `QUEUED` (freshly enqueued, reserved to `node_id`), `NULL` (unreserved, bulk/external), `CLAIMED`, `RELEASED`, or `YIELDED` (handed back voluntarily — its own status so `claim` knows not to spend budget on it, D33). |
| `node_id`           | For `QUEUED`, the enqueuing (reserving) node. For `CLAIMED`/`RELEASED`, the current/last owner. **Not cleared** on a `MultiNodeTaskLedger` retryable release — kept as "last owner" so the affinity window has something to key on. `SimpleTaskLedger` clears it on release (no affinity concept). |
| `attempt_count` | **Outer** counter (D33), 1-based. An *attempt* is one operator-initiated run: 1 for a freshly enqueued task (set to `tm_task.attempt_count + 1` when the row is created, at enqueue/rerun/promote), incremented only by `rerun`. **Unbounded** — there is no maximum for a manual action, which is why `max_attempts` was removed rather than renamed. Automatic retry never touches it. |
| `retry_count` / `max_retries` | **Inner** counter (D33), 0-based: the ordinal of the execution running *right now* within this attempt. **Spent by `claim`** — every claim bills `+1` except one resuming a `YIELDED` row — and **reset to `-1` whenever `attempt_count` increments**, so a re-run gets a full fresh budget and that attempt's first claim lands it on `0`. Billing at claim rather than release is what makes a lost lease cost anything: the node that lost it is unfenced by construction and can never write. `max_retries = N` admits ordinals `0..N`, i.e. N+1 executions per attempt — the same total the old `attempt_count > max_attempts` check gave. |
| `next_retry_at`     | Backoff gate — **discovery** skips the row until this passes (`NULL` = immediately eligible). Set on a retryable release. Claim itself does not gate on it (discovery already filtered). |
| `claimed_at`        | When the current claim started. Only bumped forward on a genuine re-claim (new owner, or the same owner reclaiming after the short write-minimization window) — see the window model below. |
| `lease_expires_at`  | Multi-node only. `HeartbeatStrategy` renews this periodically while a worker holds the task; if it lapses, another node's claim query treats the row as abandoned (the "claim timeout" window). |
| `created_at`, `updated_at` | `updated_at` drives the reservation/affinity/claim-timeout windows below. |

Recommend Postgres-specific tuning here in a consuming app's own DDL
(fillfactor, `autovacuum_vacuum_scale_factor`) if claim/renew volume is high
— that part of the toolkit's original design genuinely is deployment-specific
and stays out of taskmesh's own portable schema.

**Multi-node 4-window claim model** (`MultiNodeTaskLedger`) — discovery and
claim are driven **entirely FROM `tm_in_flight`**, with no join to `tm_task`
(a hot row exists iff the task is non-terminal, so the cold status check is
redundant). A row is eligible under any of these conditions —
1. `status IS NULL` (unreserved bulk/external row) — any node.
2. `status = QUEUED` and `node_id = me` — the reserving node claims its own
   freshly-enqueued work immediately.
3. `status = QUEUED` and (`node_id IS NULL` or past the **reservation window**,
   `updated_at < now - reservationSeconds`) — a reservation the enqueuing node
   didn't pick up in time is open to any node (so a crashed/overloaded
   producer never strands its own backlog).
4. `CLAIMED` by the *same* node (idempotent re-claim / lease renewal path).
5. `RELEASED`, owned by the same node, within the **affinity window**
   (`updated_at >= now - affinitySeconds`) — last owner gets first refusal, so
   a flaky task doesn't bounce between nodes.
6. `RELEASED` and *past* the affinity window — open to any node.
7. `CLAIMED` but past the **claim-timeout window**
   (`updated_at < now - claimTimeoutSeconds`) — the owning node is presumed
   dead; any node may steal it.

**Discovery additionally enforces the backoff gate** (`next_retry_at` null or
past); claim does not (its caller already filtered through discovery). The
**reclaim window** (`reclaimSeconds`, much shorter than affinity) only affects
whether `claimed_at` is bumped forward on the `SET` side of the claim UPDATE —
it never appears in eligibility. Mixing the windows up (letting the short
reclaim window leak into eligibility) was a real bug from prior-art history;
`findClaimableMultiNode` and `claimMultiNode` **must stay in exact sync** on
the eligibility block above, or discovery and claim drift apart.

**Release**, split by outcome:
- **Terminal** (`COMPLETED`/`FAILED`) — the hot row's final `attempt_count`/
  `retry_count`/`max_retries` are read, then the hot row is deleted, fenced on `node_id` +
  `status='CLAIMED'` (0 rows deleted means ownership was already lost, and
  `tm_task` is never touched); only then is `tm_task` updated by PK, copying
  those counters plus the terminal status/reason as the cold snapshot.
- **Retry** (retryable failure) — touches only the hot row: `MultiNodeTaskLedger`
  fences it to `RELEASED` (node_id retained) and sets `next_retry_at` from the
  ordinal `claim` is about to move the counter to. Neither counter is written
  here — billing is `claim`'s job (D33). On exhaustion it
  instead snapshots the final counters to `tm_task` (status `FAILED`) and
  deletes the hot row. `SimpleTaskLedger` resets the hot row straight to
  unclaimed (node cleared) rather than `RELEASED`, but otherwise
  increments/gates the same way.
- **Yield** (cooperative hand-back, D33) — the owner still holds the lease and
  voluntarily returns unfinished work (pause/shutdown). Fenced the same way,
  but consumes **no** retry budget and sets **no** backoff gate: the row is
  parked in `YIELDED` (reserved to the same node) with `next_retry_at`
  cleared, so it is immediately claimable again once whatever caused the yield
  clears. The distinct status is load-bearing rather than cosmetic — it is how
  the next `claim` knows this is the same execution resuming and must not be
  billed. A pause/resume loop therefore cannot bankrupt a task.

**Budget is spent at claim, and exhaustion is enforced there too** (D33).
A node that loses its lease cannot record anything — its release is unfenced
and returns `LOST_OWNERSHIP` — so if `release()` were the only place that
billed and the only place that terminated, a task that systematically cannot
finish within one lease would be reclaimed forever, never reaching a terminal
state and never appearing as a failure. Instead:

- `claimMultiNode`'s UPDATE carries
  `retry_count = retry_count + CASE WHEN status = 'YIELDED' THEN 0 ELSE 1 END`,
  atomic with the claim, no sweeper (which D27's visibility-based recovery
  deliberately avoids).
- Immediately after winning, the ledger compares the freshly billed ordinal
  against `min(max_retries, RetryPolicy.maxRetries)`. Over budget means the
  execution is **not** admitted: the cold row is snapshotted `FAILED` with
  `reason_code = RETRY_BUDGET_EXHAUSTED`, the hot row is deleted, and
  `ClaimOutcome.EXHAUSTED` is returned so the engine opens an incident and
  records the failure. The snapshot stores the last ordinal that actually ran
  — the increment that tipped the budget over belongs to an execution that
  never happens.

The check deliberately runs *after* the claim UPDATE rather than as another
condition inside it: the eligibility block stays byte-identical to
`findClaimableMultiNode`'s, and the terminal write is fenced by ownership the
claim just acquired, so no other node can be running the task while it
happens. That write joins the **caller's** transaction (D34) — the engine's, so
that the terminal write and the consumer's `onTerminal` commit as one unit. It
takes `tm_in_flight` first (from the claim UPDATE) then `tm_task`, and **every
other write path does the same** — claim, release, rerun and enqueue all touch
the hot row before the cold one. That single order is forced by this path: it
already holds the hot row before it can know the task is terminal, so it can
never take the cold one first, and anything that did would deadlock against it.
`release` used to take `tm_task` first (a `lockByPk` that is now gone) and a
Postgres torture run deadlocked 40 times out of 40 — see D34.

`release()` keeps its own exhaustion check for the ordinary path — a node that
survives and reports its failure — deciding one step early that no next
execution exists rather than letting it be claimed only to be terminated then.
Neither path increments twice: **only `claim` moves the counter.**

`SimpleTaskLedger` (single-node) skips the lease/affinity/claim-timeout/
reservation machinery entirely — `claimSingleNode` is a plain
`status IN (NULL, QUEUED) -> CLAIMED` UPDATE against the hot row alone (no
`tm_task` join), and `recoverOwnClaims` resets this node's own stale `CLAIMED`
hot rows back to unclaimed at startup (safe because a single-node deployment is
definitionally the only writer).

## `tm_history_event`

Append-only per-attempt lifecycle log, written by `JpaHistoryWriter`. One row
per significant event in a task's life — not just terminal outcomes, so this
is the table to query for "what happened to task X" over its full history.

| Column | Notes |
|---|---|
| `id` (PK) | Surrogate, `BIGSERIAL`/`IDENTITY`. |
| `task_type`, `task_id` | Which task. |
| `attempt_count` / `retry_count` | Which execution this event belongs to — the D33 pair, read as "(attempt 2, retry 3) is the third retry of the second attempt". |
| `event_type` | `STARTED`, `COMPLETED`, `FAILED`, `RETRIED`, `OWNERSHIP_LOST`, `PAUSED`, `HEARTBEAT`. |
| `detail` | Free-text detail (`LONGVARCHAR`, not `@Lob` — see Postgres note above). |
| `occurred_at` | When the event happened. |

Query by `(task_type, task_id)` for a task's full timeline; there's an index
on exactly that pair (`idx_tm_history_event_task` in the toolkit's DDL).

**`taskmesh-max` only** (D32): a `partition_key` column, part of the PK
alongside `id` — required, since `id`'s generation moves from `IDENTITY` to an
explicit `SEQUENCE` there (Hibernate doesn't support `IDENTITY` inside a
composite `@IdClass` key).

## `tm_audit_event`

Operator-triggered and cluster-level events only — **not** routine per-task
transitions (those go to `tm_history_event`). Written by `JpaAuditEventSink`,
called from exactly one place in the engine today: `TaskMeshEngine.pause(...)`
/ `.resume(...)`.

| Column | Notes |
|---|---|
| `id` (PK) | Surrogate. |
| `event_type` | e.g. `PAUSE_REQUESTED`, `RESUME_REQUESTED`. |
| `task_type`, `task_id` | Nullable — cluster-wide events (like pause) have no single task. |
| `actor_type` | `APPLICATION_USER`, `SYSTEM`, or `ENGINE_MODULE`. |
| `performed_by` | Free-text identity of who/what triggered it (e.g. a REST caller's `performedBy` string). |
| `message` | Human-readable detail (`LONGVARCHAR`). |
| `occurred_at` | When. |

This is the table to point a compliance/audit view at — it's deliberately
small-volume (operator actions only), unlike `tm_history_event` which grows
with every task attempt.

**Not touched by D32** — no `partition_key` here in any tier. Deferred
(revisited separately), mainly because of the nullable-`task_id` cluster-wide
event case (`PAUSE_REQUESTED`/`RESUME_REQUESTED` have no task, so no natural
partition key either).

## `tm_incident`

Terminally-failed tasks that need a human to look at them — written by
`JpaIncidentStore` once a task exhausts `max_retries` (or hits a
non-retryable failure). This is the operational worklist: "what's broken and
needs attention," separate from `tm_task`'s `status='FAILED'` rows (which are
the raw state, not a triaged queue).

| Column | Notes |
|---|---|
| `id` (PK) | Surrogate. |
| `task_type`, `task_id` | Which task. |
| `reason_code` | Short machine-readable failure classification. |
| `message` | Full error detail (`LONGVARCHAR`). |
| `created_at` | When the incident was raised. |
| `resolved_at`, `resolved_by` | Both `NULL` while open. An operator (or a rerun that subsequently succeeds) resolves it by setting these — taskmesh itself doesn't auto-resolve incidents. |

A partial index on open incidents (`WHERE resolved_at IS NULL`) keeps the
"what's currently broken" query cheap regardless of how much resolved history
accumulates — see `idx_tm_incident_open` in the toolkit's DDL.

**`taskmesh-max` only** (D32): same `partition_key`-joins-`id`-in-the-PK
treatment as `tm_history_event` above, same `SEQUENCE`-not-`IDENTITY` reason.

## `tm_attempt_history`

BATCH profile's coarse alternative to `tm_history_event` (`AttemptHistoryWriter`)
— one row per `(task_type, task_id, attempt_count, retry_count)`, upserted in
place (`INSERT` on `STARTED`, `UPDATE` on the execution's terminal outcome)
rather than append-only.

| Column | Notes |
|---|---|
| `task_type`, `task_id`, `attempt_count`, `retry_count` (PK) | One row per execution — the composite key is load-bearing twice over: `AttemptHistoryWriter` looks up by it to decide insert vs. update, and having **both** D33 counters in it is what makes `rerun`'s `retry_count` reset safe — `(1, 3)` and `(2, 0)` are distinct by construction, so a fresh attempt can never overwrite pre-rerun history. |
| `status` | Last `HistoryEventType` recorded for the attempt. |
| `started_at`, `ended_at` | `ended_at` set only on a terminal outcome. |
| `node_id` | The node that recorded the attempt's most recent event — set from `HistoryEvent.nodeId()` on every write (insert and update), so it reflects the current owner, not just who started it. |
| `detail` | Free-text detail. |

**`taskmesh-max` only** (D32): `partition_key` joins the PK (now four
columns) — required, since this table is physically partitioned at this tier
and Postgres requires the partition column in every unique constraint.

## `tm_control_flag`

Single-row cluster pause flag, read by `JpaPauseController` on a poll
interval and written by `TaskMeshEngine.pause()`/`.resume()`. Every node in a
multi-node deployment polls the same row, so a pause requested from any one
node (e.g. via a REST endpoint) takes effect cluster-wide within one poll
interval.

| Column | Notes |
|---|---|
| `id` (PK) | Always `1` — this table only ever has one row. |
| `pause_requested` | `true` while the cluster is paused. Workers finish their current task but the poller stops claiming new ones. |

The single row must be seeded (`INSERT INTO tm_control_flag (id, pause_requested) VALUES (1, false)`)
as part of initial setup — `JpaPauseController` reads it, it doesn't create it.
