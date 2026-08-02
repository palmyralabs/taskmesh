# Module layout (proposed)

```
taskmesh-api                  interfaces + domain types only. No Spring, no JDBC,
                               no framework dependency beyond the JDK (+ SLF4J for
                               logging contracts if needed). Every interface in
                               interfaces.md lives here.

taskmesh-engine                the "driving implementation": feeder, claim loop,
                               dispatcher, worker pools. Wires the -api interfaces
                               together; has no idea what any TaskProcessor
                               actually does. Depends on -api only — plain Java
                               (ExecutorService-based), zero Spring/Spring Boot
                               dependency. ProcessorRegistry accepts a
                               List<TaskProcessor> via plain constructor
                               injection or java.util.ServiceLoader discovery;
                               no DI container required. The transaction-
                               boundary open question (Q-TXBOUNDARY) affects
                               how this module invokes processors — default
                               TransactionRunner is plain-JDBC-based, not
                               Spring-@Transactional-based, to keep this
                               module framework-agnostic.

taskmesh-spring-boot-starter    OPTIONAL adapter for consumers running Spring
                               Boot (which, per this org's stack, is most real
                               consumers). Provides: @ConfigurationProperties
                               binding for every profile's config (per
                               configuration.md), @Component-based
                               ProcessorRegistry auto-population, auto-
                               configuration wiring. No transaction-related
                               component — transaction management is entirely
                               the TaskProcessor author's own concern (D15),
                               not something taskmesh brokers even optionally.
                               A consumer NOT using Spring Boot skips this
                               module entirely and wires taskmesh-engine
                               directly (constructor injection + config
                               builders) — core functionality is identical
                               either way, per configuration.md's dual-path rule.

taskmesh-postgres              reference implementation of the persistence-backed
                               interfaces (ClaimStrategy, HistoryWriter,
                               AuditEventSink, IncidentStore) against Postgres —
                               the hot/cold table split from task-execution.md
                               lives here.

taskmesh-metrics-micrometer    MetricsReporter implementation backed by Micrometer.
                               A no-op default lives in -engine or -api itself so
                               this module is opt-in.

taskmesh-testkit               in-memory / hand-fakeable implementations of every
                               -api interface, for unit-testing a TaskProcessor
                               without a DB or Spring context — mirrors the
                               Mockito-plus-hand-built-context pattern both
                               nm_cleanser and palmyra76/workflow converged on.

taskmesh-devtools              local-dev support: H2-backed profile, a single-task
                               debug endpoint (submit one task_type + payload,
                               watch it run, skip claim/feeder/lease machinery) —
                               modeled on nm_cleanser's DevProcessorController.

taskmesh-profile-batch          thin aggregator: taskmesh-engine + tuned defaults
                               for the BATCH workload profile (long-running,
                               low-concurrency).
taskmesh-profile-highvolume     same, for HIGH_VOLUME (medium-running,
                               high-concurrency — this project's own origin
                               consumer, policy migration, uses this one).
taskmesh-profile-reactive       same, for REACTIVE (short-running, low-latency).
```

See [`profiles.md`](profiles.md) for what each profile module actually
configures. These are config/defaults aggregators, not code forks — the
underlying implementation classes (`LeaseClaimStrategy` etc.) live once, in
their normal infra module, and profile modules just wire them differently.
More profiles can be added later without touching existing ones.

## What does NOT live here

Anything that implements `TaskProcessor` for a real domain (e.g. policy
migration) lives in the **consuming project**, not in taskmesh. taskmesh never
depends on a consumer; consumers depend on `taskmesh-api` + `taskmesh-engine` +
one persistence implementation.

## Dependency direction

```
consumer's TaskProcessor impls
        │
        ▼
  taskmesh-engine  ──depends on──▶  taskmesh-api
        │                                ▲
        ▼                                │
  taskmesh-postgres ─────────────────────┘
  taskmesh-metrics-micrometer ────────────┘
  taskmesh-testkit ────────────────────────┘

  taskmesh-spring-boot-starter ──depends on──▶ taskmesh-engine
        (optional; a consumer either uses this, or wires -engine directly —
         never both, never required)
```

`-api` has no outgoing dependency on any other taskmesh module — it's the
stable core everything else compiles against. `-engine` has no dependency on
Spring/Spring Boot at all — the starter module depends on `-engine`, not the
other way around, so core functionality never requires the starter to exist.

## Open question this layout doesn't yet answer

Whether `-engine` should itself be replaceable (a second "driving
implementation" of the orchestration loop) or whether it's meant to be the
one true implementation with only its *collaborators* (claim strategy,
history writer, etc.) pluggable. Current lean: `-engine` is not pluggable —
only what it depends on is. Revisit if a real need for a second engine
implementation ever appears.
