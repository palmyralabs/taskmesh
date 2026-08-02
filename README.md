# taskmesh

A reusable, multi-node task orchestration engine: claim-based, DB-coordinated,
shared-nothing across nodes. One pluggable `TaskProcessor` per task type;
the engine (feeder, claim/lease, retry, pause, telemetry) is domain-agnostic.

Originated from the task-execution design in the HPE/LIC data migration
toolkit (`docs/planning/task-execution.md` there), generalized so any
project needing "run many independent units of work in parallel across
nodes, safely" can depend on it instead of re-deriving the same claim/lease
machinery.

## Status

Planning stage. No source code yet — see [`docs/planning/`](docs/planning/README.md)
for the design in progress. Code lands after the interface/module design is settled.

## Source of truth

`docs/planning/` is authoritative. This README is a pointer, not a summary —
update the planning docs first.
