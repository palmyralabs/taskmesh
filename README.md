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

Under active development. Source lives in [`source/`](source/) as a multi-module
Gradle build (Java 17):

- `taskmesh-api` — interfaces + domain types only (JDK + SLF4J).
- `taskmesh-engine` — the driving implementation (feeder, claim loop, dispatcher,
  worker pool); plain Java, no Spring.
- `taskmesh-impl` — JPA-backed persistence of the `-api` contracts (the `tm_*`
  tables; see [`docs/schema.md`](docs/schema.md)).
- `taskmesh-spring-boot-starter` — optional Spring Boot auto-configuration.
- `taskmesh-testkit` — in-memory fakes for unit-testing a `TaskProcessor`.

Build and test from `source/`: `./gradlew build`.

## Source of truth

`docs/planning/` holds the authoritative design discussion and
[`docs/schema.md`](docs/schema.md) documents the persistence tables. Where a
planning doc disagrees with the code (e.g. the persistence module is
`taskmesh-impl`, not the earlier-proposed `taskmesh-postgres`), the code and
`docs/schema.md` win.
