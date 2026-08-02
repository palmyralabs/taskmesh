# taskmesh planning docs

Source of truth for the design. Layout:

- [`overview.md`](overview.md) — mission, origin, what "reusable" means here
- [`module-layout.md`](module-layout.md) — proposed sub-project structure (api / engine / impls)
- [`interfaces.md`](interfaces.md) — the dimension → interface decomposition; one section per
  interface, with candidate implementations and open behavioral questions per interface
- [`infra-implementations.md`](infra-implementations.md) — concrete implementation specs for
  the infra-tier interfaces (ClaimStrategy, SchedulingStrategy, HeartbeatStrategy,
  PauseController, HistoryWriter, AuditEventSink, IncidentStore, MetricsReporter) — each with
  a cleanser-style and a workflow-style variant
- [`claim-strategy.md`](claim-strategy.md) — full detailed spec for ClaimStrategy specifically
  (interface contract, exact SQL, schema, edge cases, config) — the load-bearing interface
  everything else in the infra tier depends on
- [`profiles.md`](profiles.md) — the three workload profiles (BATCH / HIGH_VOLUME / REACTIVE)
  that drive which implementation variant fits which deployment, and the per-interface
  selection matrix
- [`configuration.md`](configuration.md) — dual-path config rule (YAML/Spring Boot binding +
  plain builder), applies to every config surface across all profiles
- [`runtime-flow.md`](runtime-flow.md) — end-to-end lifecycle walkthrough tying every interface
  together (startup → discovery → claim → execute → success/failure/pause/lost-ownership →
  recovery), written to validate the whole design composes before drilling into individual
  implementations further
- [`decisions.md`](decisions.md) — resolved design decisions, with the reasoning behind each
- [`open-questions.md`](open-questions.md) — unresolved items, tracked so nothing gets lost
- [`comparison/`](comparison/README.md) — research on peer designs (nm_cleanser, the HPE/LIC
  toolkit's task-execution.md, palmyra76/workflow) that this design draws from

## How this project came to exist

Grilled out of a planning session for the HPE/LIC data migration toolkit
(`E:\mydev\customer\hpe\lic\data_migration_toolkit`), which needed a multi-node,
policy-folder-unit-of-work migration engine. Partway through, the scope
deliberately generalized: instead of a migration-specific engine, build a
domain-agnostic task orchestration engine, with policy migration as its first
consumer (a `TaskProcessor` implementation living in the toolkit repo, not here).

Two existing sibling codebases were used as reference points during design:
- **nm_cleanser** (`E:\mydev\customer\senthil\nm_cleanser`) — single-JVM pipeline
  orchestrator, strong on processor contract / dev-experience patterns.
- **palmyra76/workflow** (`E:\mydev\palmyra76\workflow`) — multi-node BPMN engine,
  strong on claim/lease and failure-classification patterns.

Neither is a dependency of taskmesh — they're prior art, read for patterns to
reuse or deliberately avoid. See `comparison/` for the detailed findings.
