# Mission

A reusable engine for running many independent **units of work** ("tasks") in
parallel, across multiple nodes, safely — without every consuming project
re-deriving claim/lease/retry/pause machinery from scratch.

## What "reusable" means here

The engine core (feeder, claim/lease, retry policy, pause, telemetry) knows
nothing about what a task actually does. A task is `(task_type, task_id,
payload)`. Domain logic lives entirely behind a `TaskProcessor` implementation,
registered by `task_type`, supplied by the consuming project — not by taskmesh.

taskmesh's first real consumer is the HPE/LIC data migration toolkit's policy
migration (`task_type=POLICY_MIGRATE`, `task_id=policyNumber`), but the engine
must not encode anything policy- or migration-specific.

## Non-negotiables (carried over from the originating design)

- **Multi-node, shared-nothing, DB-coordinated.** No ZooKeeper/Redis dependency
  for the core claim mechanism.
- **Idempotent at the task level.** Any task must be safely re-runnable; the
  engine guarantees at-least-once dispatch, never exactly-once — the processor
  owns dedup, using the stable `task_id` the engine provides.
- **Claim is the only cross-node gate.** One atomic, WHERE-guarded UPDATE;
  0 rows affected = lost the race, back off silently. No secondary lock table,
  no external coordinator.
- **Retry the largest safe atomic unit, never a partial in-flight sub-step.**
  What that unit is varies by task_type; the engine does not assume sub-task
  checkpointing exists.
- **Unrecoverable failures fail fast.** Retryable and non-retryable failures
  are classified, not lumped together — a task should not burn its retry
  budget on a failure that will never succeed.
- **Silence is a bug.** Unregistered task types, lost claims, and classification
  gaps must be loud (logged, incident-worthy) — never a silent no-op.

## What this project is NOT

- Not a workflow/BPMN engine. No multi-step process graphs, no sequencing
  between task types. If a consumer needs a pipeline of steps, that's their
  `TaskProcessor` internally orchestrating sub-work, not a taskmesh concept.
  (Revisit only if a second real consumer genuinely needs it — see
  `open-questions.md`.)
- Not a message queue / event bus. Tasks are pulled by feeders polling a
  DB-backed queue table, not pushed via pub/sub.
- Not a scheduler for time-based/cron work. Tasks are queued externally by
  the consuming project; taskmesh only handles claiming and executing what's
  already queued.
