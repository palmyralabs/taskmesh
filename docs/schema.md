# taskmesh-impl: `tm_*` table reference

This document describes the six tables owned by `taskmesh-impl` (package
`com.palmyralabs.taskmesh.impl`, entities under `.entity`, repositories
under `.repository` — D29) — the JPA-backed persistence implementation of
taskmesh's `-api` contracts (`TaskLedger`, `ClaimStrategy`, `HeartbeatStrategy`,
`HistoryWriter`, `AuditEventSink`, `IncidentStore`, `PauseController`), now
housing all three workload profiles (BATCH/HIGH_VOLUME/REACTIVE) rather than
just HIGH_VOLUME.

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
**entire lifetime**, touched only at enqueue and on terminal/requeue
transitions. Deliberately carries **no payload column**: payload resolution
is entirely the consuming application's `PayloadProviderFactory`/
`PayloadProvider` concern, never taskmesh's to store.

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
| `status`            | `QUEUED`, `COMPLETED`, or `FAILED` only.                                                |
| `attempt_count` / `max_attempts` | Retry bookkeeping. Exhaustion (`attempt_count >= max_attempts`) is checked pessimistically via `findForUpdate` before a retryable failure is turned into a terminal one. |
| `next_retry_at`     | Backoff gate for `QUEUED` rows — discovery/claim queries skip a row until this passes. `NULL` means immediately eligible. |
| `reason_code`, `error_summary` | Set on terminal transitions (`COMPLETED` clears them; `FAILED` records why). |
| `created_at`, `updated_at` | Standard audit timestamps. |

## `tm_in_flight` (hot)

The claim-state table — all claim/renew/release churn lands here, never on
`tm_task`. One row per task that's been enqueued and isn't yet terminal:
created **eagerly, alongside the `tm_task` row, at enqueue time**
(`status = NULL`, unclaimed — not lazily staged on first claim attempt like
the toolkit's original `in_flight` table), transitions to `CLAIMED`/
`RELEASED` as a `ClaimStrategy` drives it, and is **deleted entirely** the
moment the task reaches a terminal outcome (`COMPLETED`, `FAILED`, including
retry exhaustion) — so this table's steady-state size tracks current
backlog+active count, never all-time history.

| Column             | Notes                                                                                  |
|---------------------|-----------------------------------------------------------------------------------------|
| `task_type`, `task_id` (PK) | Same identity as the paired `tm_task` row. |
| `status`            | `NULL` (unclaimed/staged), `CLAIMED`, or `RELEASED`.                                    |
| `node_id`           | Current or last owner. **Not cleared** on a `MultiNodeTaskLedger` retryable release — kept as "last owner" metadata (paired with `status='RELEASED'`) so the affinity window has something to key on. `SimpleTaskLedger` clears it immediately on release (no affinity concept). |
| `claimed_at`        | When the current claim started. Only bumped forward on a genuine re-claim (new owner, or the same owner reclaiming after the short write-minimization window) — see the 3-window model below. |
| `lease_expires_at`  | Multi-node only. `HeartbeatStrategy` renews this periodically while a worker holds the task; if it lapses, another node's claim query treats the row as abandoned (the "claim timeout" window). |
| `created_at`, `updated_at` | `updated_at` drives the affinity/claim-timeout windows below. |

Recommend Postgres-specific tuning here in a consuming app's own DDL
(fillfactor, `autovacuum_vacuum_scale_factor`) if claim/renew volume is high
— that part of the toolkit's original design genuinely is deployment-specific
and stays out of taskmesh's own portable schema.

**Multi-node 3-window claim model** (`MultiNodeTaskLedger`) — discovery is
driven **FROM `tm_in_flight`**, joined out to `tm_task` for status/
`next_retry_at` (not the other way round, so the scan starts from the table
sized by current backlog, not all-time history). A claim query is eligible
under any of these conditions —
1. `tm_task.status = QUEUED` and due (`next_retry_at` null or past) — always
   required, regardless of which case below applies to the hot row.
2. Hot row `status IS NULL` (never claimed).
3. `CLAIMED` by the *same* node (idempotent re-claim / lease renewal path).
4. `RELEASED`, owned by the same node, and within the **affinity window**
   (`updated_at >= now - affinitySeconds`) — gives the last owner first
   refusal on a task it just released, so a single flaky task doesn't bounce
   between nodes.
5. `RELEASED` and *past* the affinity window — open to any node.
6. `CLAIMED` but past the **claim-timeout window**
   (`updated_at < now - claimTimeoutSeconds`) — the owning node is presumed
   dead; any node may steal it.

The **reclaim window** (`reclaimSeconds`, much shorter than the affinity
window) only affects whether `claimed_at` is bumped forward on the `SET`
side of the claim UPDATE — it never appears in the eligibility `WHERE`
clause. Mixing the two up (letting the short reclaim window leak into
eligibility) was a real bug pulled from prior-art history; `findClaimableMultiNode`
and `claimMultiNode` must stay in exact sync on eligibility, or discovery and
claim drift apart — now doubly true since both queries also have to stay in
sync on *which table drives the join*.

**Release**, split by outcome:
- **Terminal** (`COMPLETED`/`FAILED`) — the hot row is deleted first, fenced
  on `node_id` + `status='CLAIMED'` (0 rows deleted means ownership was
  already lost, and `tm_task` is never touched); only then is `tm_task`
  updated, unconditionally by PK — ownership fencing lives entirely on the
  hot-table op now.
- **Requeue** (retryable) — `MultiNodeTaskLedger` sets the hot row to
  `RELEASED` (node_id retained); on exhaustion the hot row is deleted right
  after (terminal — the just-set `RELEASED` no longer means anything).
  `SimpleTaskLedger` resets the hot row straight to unclaimed instead.

`SimpleTaskLedger` (single-node) skips the lease/affinity/claim-timeout
machinery entirely — `claimSingleNode` is a plain `status IS NULL -> CLAIMED`
UPDATE (still joined to `tm_task.status = QUEUED` via `EXISTS`), and
`recoverOwnClaims` resets this node's own stale `CLAIMED` hot rows back to
unclaimed at startup (safe because a single-node deployment is definitionally
the only writer).

## `tm_history_event`

Append-only per-attempt lifecycle log, written by `JpaHistoryWriter`. One row
per significant event in a task's life — not just terminal outcomes, so this
is the table to query for "what happened to task X" over its full history.

| Column | Notes |
|---|---|
| `id` (PK) | Surrogate, `BIGSERIAL`/`IDENTITY`. |
| `task_type`, `task_id` | Which task. |
| `attempt_number` | Which attempt this event belongs to (starts at 1). |
| `event_type` | `STARTED`, `COMPLETED`, `FAILED`, `RETRIED`, `OWNERSHIP_LOST`, `PAUSED`, `HEARTBEAT`. |
| `detail` | Free-text detail (`LONGVARCHAR`, not `@Lob` — see Postgres note above). |
| `occurred_at` | When the event happened. |

Query by `(task_type, task_id)` for a task's full timeline; there's an index
on exactly that pair (`idx_tm_history_event_task` in the toolkit's DDL).

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

## `tm_incident`

Terminally-failed tasks that need a human to look at them — written by
`JpaIncidentStore` once a task exhausts `max_attempts` (or hits a
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
