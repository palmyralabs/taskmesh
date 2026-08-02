# Open questions

Unresolved items surfaced during design. Resolved items move to `decisions.md`.

## Q-CAP — Cluster-wide concurrency cap

Neither task-execution.md nor palmyra76/workflow solved a true cluster-wide
cap on total in-flight tasks — both only have per-node pool sizing. Is a
cluster-wide cap actually needed for taskmesh, or is per-node sizing
sufficient given the claim mechanism already throttles naturally (a node
only claims what its pool can drain)?

## Q-PAYLOAD-VERSION — Payload schema versioning

Given the opaque JSONB-ish payload design (D2), should `schema_version` be a
column from day one even if versioning logic is deferred? Neither peer
project solved in-flight schema migration (workflow names it as an open,
unbuilt admin operation; nm_cleanser doesn't address it at all) — this needs
fresh design, not a borrowed pattern.

## Q-TIMEOUT — Per-task_type timeout enforcement

None of the three peer designs has "configurable per-task-type timeout,
actually enforced" working today (nm_cleanser: none; workflow: one hardcoded
global value despite a richer unimplemented design; task-execution.md: only
covers one HTTP call's socket timeout). workflow's unbuilt config surface
(`claim-timeout-seconds` etc.) is a usable blueprint. Where does `TimeoutPolicy`
plug into the engine's execution loop, and what's the default?

**Enforcement mechanism, found while writing `runtime-flow.md`**: even once
a default timeout value exists, *forcibly* reclaiming a thread blocked
inside one long-running call isn't reliably possible in Java
(`Thread.interrupt()` doesn't preempt blocking I/O) — the same limitation
Attack 3 surfaced for checkpoint frequency. Practical fallback for now:
`TimeoutPolicy` is a **detection** mechanism (flag an incident once a task
exceeds its declared timeout), not a **preemption** mechanism. Still open:
does detection alone satisfy the requirement, or is there a real need for
actual preemption (e.g. via a cancellable I/O client the processor is
required to use)?

## Q-MULTISTEP — Multi-step task types (future)

Both nm_cleanser (run-plan table) and palmyra76/workflow (BPMN execution
tree) support multi-step units of work; taskmesh's core deliberately doesn't
(see overview.md "What this project is NOT"). Revisit only if a second real
consumer genuinely needs internal sub-step sequencing — until then, a
`TaskProcessor` that needs multiple steps just does them sequentially inside
its own `process()` call.

## Q-ATTEMPTNUM — `HistoryEvent.attemptNumber` has no authoritative source at `-engine`

`HistoryEvent(TaskRef, int attemptNumber, HistoryEventType, String detail,
Instant occurredAt)` requires an attempt number, but D16 keeps the real
`attempt_count` counter entirely inside `ClaimStrategy`'s own storage,
updated atomically inside `release()` — no interface exposes it back to the
engine (`ClaimOutcome`/`RenewOutcome`/`ReleaseOutcome` are all bare enums,
now `EXHAUSTED`-aware per D19 but still not attempt-count-aware). Current
`-engine` implementation tracks a **best-effort, engine-local, non-authoritative**
counter (in-memory, per `TaskRef`, incremented each time *this node's*
dispatch loop attempts that ref) purely so `HistoryEvent` has something to
carry — it will disagree with the real count after a cross-node retry or a
node restart. Open: should `ClaimStrategy.claim()`/`release()` return the
authoritative attempt number so `HistoryWriter` entries are trustworthy, or
is best-effort acceptable given `HistoryWriter` is audit-only (never read by
control flow, per its own Javadoc) and a slightly-off attempt number in an
audit log is low-stakes?

## Q-OBSERVABILITY — Metrics/observability design

Both peer projects have essentially no working metrics layer (Actuator
present but unconfigured in nm_cleanser; nothing at all in workflow) — this
is unclaimed design territory, not a borrowed pattern. What does
`MetricsReporter` actually expose, and does the originating consumer's
"streaming observability via Kafka, not polling" goal belong in taskmesh's
core or stays a consumer-side concern layered on top of `HistoryWriter`?

