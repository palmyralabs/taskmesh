# Requirement: a read/query API on the engine

> **Status: not built — deferred.** A first cut implemented all three items
> across all three tiers (~2,200 lines) and was discarded as too large for what
> is being asked. Scoping conclusions worth keeping for the revisit:
>
> - Build **item 3 only**. Items 1 and 2 are deprioritized by this document's
>   own reasoning, and adding them later is additive if the interface's methods
>   default to `UnsupportedOperationException`.
> - That needs roughly **four API types** — the interface, a claimed-task
>   record, a filter, and a page. `TaskState`/`TaskSummary`/`TaskStateCount`
>   exist only to serve items 1 and 2; a cursor type belongs folded into the
>   page type, not standing alone in `-api`.
> - D31's three-tier duplication multiplies anything added here by three, plus
>   a test class each. Scope accordingly.

`TaskMeshEngine` is **command-only** today — `start`, `stop`, `pause`, `resume`, `rerun`,
`resolveIncident`, `enqueue`, `execute`, `adjustPoolSize`, `currentPoolSize`. There is no way to
*ask* it anything.

So every consumer that needs operational status has exactly one option: query `tm_task` and
`tm_in_flight` directly. That is what the LIC migration toolkit does today, and it is a coupling
taskmesh should not be forcing.

## Why this matters — it has already broken consumers twice

Both in a single day, from ordinary internal changes:

- **D33** renamed `max_attempts` → `max_retries` and split the counters. Consumer code calling
  `TmTaskEntity.getMaxAttempts()` stopped compiling.
- **Moving tier** `-pro` → `-max` changed the package of `TaskStatus`, `TmTaskEntity`,
  `TmInFlightEntity` and every repository, so ~19 import sites had to be rewritten.

Neither change was wrong. Both were invisible to taskmesh because the coupling is undeclared: the
tables are an implementation detail that consumers have no sanctioned alternative to reading. A
published read API would have absorbed both.

## What consumers actually need

Taken from the toolkit's real dashboard, in priority order:

1. **Counts by status, per task type** — the headline "how much is queued/completed/failed".
2. **Tasks in a given status** — the operator worklist. Needs paging: a status can hold millions.
3. **Currently claimed tasks** — task, node, `claimed_at`, `lease_expires_at`. This backs "what is
   each node working on" and the stuck-task list (`now() - claimed_at > threshold`).

**Item 3 is the load-bearing one**, because it is the only one a consumer cannot reasonably provide
for itself. A consumer *can* maintain its own projection of terminal state — the toolkit does, writing
its own row once per attempt. But it cannot project *claim* state without a row write per claim, and
at ~1.5B tasks that is billions of dead tuples on a table it also has to scan. So live claim state
has to come from the engine or not at all.

Items 1 and 2 are lower priority precisely because a consumer holding its own projection can serve
them better — with its own domain columns attached, which `tm_task` does not have.

## Shape

- **Return API-level records, not entities.** Handing back `TmTaskEntity` would re-create the same
  coupling with extra steps: the entity is the schema. A small record per query keeps the internal
  shape free to change.
- **Page the list queries.** Keyset or cursor; an unpaged "all FAILED tasks" is not usable at scale.
- **Filter by `partitionKey`** where the tier has one (D32) — without it a partitioned consumer
  cannot prune, which is most of why the partition key exists.
- Consider whether this belongs on `TaskMeshEngine` or as a separate `TaskQuery` SPI alongside it.
  A separate interface keeps the command surface intact and lets a tier decline to implement the
  expensive parts.

## Interim position in the toolkit

Until this exists, the toolkit:

- reads its **own** projection for counts and worklists — better anyway, since it carries division
  and branch, which `tm_task` cannot;
- reads `tm_in_flight` directly for live claims only, contained in a single adapter and documented
  as the one sanctioned exception.

That containment is what this API would let it delete.
