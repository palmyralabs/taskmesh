# Configuration — dual path (declarative + programmatic)

Cross-cutting rule for every config surface introduced so far (`ClaimStrategy`
lease settings, `SchedulingStrategy`/`TaskQueue` sizing in `profiles.md`,
future `TimeoutPolicy`/`RetryPolicy` knobs): **every config class must be
usable both ways** — bound from `application.yaml`/env vars for consumers
running as a normal Spring Boot app, and constructed/overridden
programmatically for consumers embedding taskmesh differently (a non-Spring
host, a test harness, or just a runtime override that shouldn't require a
redeploy to change a YAML file).

## Shape

Config classes are plain POJOs/records with a builder — **not** Spring-only
`@ConfigurationProperties`-annotated classes with no other way to construct
them. The same class serves both paths:

```
@ConfigurationProperties(prefix = "taskmesh.scheduling")
public class SchedulingConfig {
    private int workerPoolSize = 375;   // shipped profile default
    private int batchSize = 500;
    // ... standard getters/setters for YAML binding, PLUS:

    public static Builder builder() { ... }   // for programmatic construction
}
```

- **YAML/env path**: `taskmesh.scheduling.worker-pool-size: 200` in
  `application.yml` — standard Spring Boot relaxed binding, works exactly
  like any other `@ConfigurationProperties` class.
- **Programmatic path**: `SchedulingConfig.builder().workerPoolSize(200).build()`
  — for a consumer that wants to compute a value at startup (e.g. size the
  pool off `Runtime.availableProcessors()` or a value read from its own
  config system), or for `taskmesh-testkit` constructing a config directly
  in a unit test with no Spring context at all.

## Precedence when both are present

Standard Spring override semantics apply: an explicitly-defined `@Bean`
(programmatic) wins over the auto-configured `@ConfigurationProperties`
bean (YAML-bound). A consumer that wants "YAML as the normal path, with a
programmatic escape hatch for special cases" gets that for free — they only
need to define their own bean when they actually want to override, not
declare intent up front.

## Applies to every profile module

`taskmesh-profile-batch`/`-highvolume`/`-reactive` each ship
`@ConfigurationProperties`-bound defaults per `profiles.md`'s tables — all
of them overridable either way, per-property, without needing to fork the
profile module or hand-wire every other setting just to change one value.

## Non-Spring embedding is the native path, not a fallback

Per D14, `taskmesh-engine` core has zero Spring/Spring Boot dependency — the
builder path isn't a workaround for an edge case, it's how core actually
works. `taskmesh-spring-boot-starter` is the layer bolted on top for
consumers who want YAML binding and auto-discovery, not the other way
around. `taskmesh-testkit` is the clearest example: no Spring context, ever,
by design.

Every config class's builder must produce a fully valid instance with the
same shipped defaults `application.yaml` binding (via the starter) would
have produced, so behavior doesn't silently diverge between the two paths —
the starter's job is convenience, not different defaults or different
semantics.
