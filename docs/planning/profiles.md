# Workload profiles

Organizing principle for picking implementations, introduced to replace the
weaker "cleanser-style vs. workflow-style" framing in `infra-implementations.md`.
The origin project each variant borrowed from was never really the point —
the *workload shape* is. Two peer projects happened to sit at two different
points on that shape; this repo's own origin consumer (task-execution.md /
policy migration) sits at a third, distinct point that had been silently
merged into "workflow-style" and shouldn't have been.

**Registry, not a fixed enum.** New profiles get added as new workload shapes
show up — this list is not closed.

## Current three profiles

| Profile | Duration | Concurrency | Reference | Origin need |
|---|---|---|---|---|
| **`BATCH`** | long-running (hours+) | low (few parallel — nm_cleanser caps at 4) | nm_cleanser | A handful of heavy, long-lived jobs; overhead of frequent polling/renewal buys nothing. |
| **`HIGH_VOLUME`** | medium (minutes — a policy: 30–50 docs) | high (target: 87–174 BOs/weekend across N nodes) | task-execution.md — **this project's actual origin consumer** | Many concurrent, moderately-sized units; needs real multi-node throughput, not just correctness. |
| **`REACTIVE`** | short (seconds) | very high, fast turnover | palmyra76/workflow | Small, fast units; latency to pick up new work matters more than per-unit efficiency. |

## Per-interface variant selection by profile

| Interface | `BATCH` | `HIGH_VOLUME` | `REACTIVE` |
|---|---|---|---|
| `ClaimStrategy` | `SingleNodeClaimStrategy` (if truly single-node) or `LeaseClaimStrategy` with a long lease | `LeaseClaimStrategy`, lease sized to the largest single task (task-execution.md default: 5min) | `LeaseClaimStrategy`, short lease (units complete in seconds, long lease just delays reclaim of genuinely dead work) |
| `TaskQueue` *(optional — see below)* | not used; `SchedulingStrategy` queries `tm_task` directly | not used by default either; `PostgresTaskQueue` is the one profile where the extension is most *likely* to eventually get invoked if backlog-depth ever demands it — but not a current requirement | not used; short-lived units, backlog churns fast |
| `SchedulingStrategy` | `FixedIntervalSchedulingStrategy`, slow cadence (5s+ is plenty when little changes per cycle) | **Needs its own tuning** — `QueueBackedSchedulingStrategy`'s mechanics (batch fetch, blocking submit) fit, but batch size / poll cadence should be tuned to policy-migration's actual throughput target, not inherited untouched from `REACTIVE`'s numbers | `QueueBackedSchedulingStrategy` as currently specified (inline continuation + fast 30s-ish safety-net poll — tuned for workflow's actual short-unit shape) |
| `HeartbeatStrategy` | `ExplicitHeartbeatStrategy` optional — long jobs benefit from active hang detection since a stuck job is expensive to lose | `UnifiedRenewalHeartbeatStrategy`, checkpoint-triggered (per the corrected spec — renewal at per-document boundaries past 50% lease) | `UnifiedRenewalHeartbeatStrategy` — units are short enough that lease expiry alone is fast enough to notice a hang |
| `HistoryWriter` | `AttemptHistoryWriter` (coarse, one row per attempt is enough for a handful of long jobs) | `LeveledEventHistoryWriter` at `STANDARD` (matches the Kafka-durable-audit-log requirement) | `LeveledEventHistoryWriter` at `MINIMAL` (high event volume from many short units — avoid write amplification) |
| `PauseController` | `ClusterFlagPauseController` (few jobs, cluster-wide pause is simple and sufficient) | `ScopedPauseController` (need to pause one `task_type` — e.g. `POLICY_MIGRATE` — without stopping `VERIFY`) | `ClusterFlagPauseController` (pausing one REACTIVE task type mid-flight is rarely meaningful given how short units are) |

`TaskQueue` is an **optional extension**, not a required interface any
implementation must go through — see `interfaces.md`. It's listed in the
matrix only to record that none of the three profiles currently need it;
all default `SchedulingStrategy` implementations query `tm_task` directly.

`IncidentStore`, `AuditEventSink`, `MetricsReporter` are not meaningfully
profile-differentiated — their recommended defaults from `infra-implementations.md`
(`DedicatedIncidentStore`, `ActorTaggedAuditEventSink`, auto-configured
`MetricsReporter`) apply across all three profiles as-is.

## HIGH_VOLUME default config

`taskmesh-profile-highvolume` ships these as `@ConfigurationProperties`
defaults (standard Spring Boot pattern, matches task-execution.md's own
"properties, tune freely" convention) — **constants at startup, not
hardcoded**: a consuming project overrides any of them via normal Spring
config (`application.yml`, env vars, profile-specific overrides) at
initialization, no code change required.

| Property | Default | Derivation |
|---|---|---|
| `taskmesh.claim.lease.durationSeconds` | 300 | task-execution.md's original value |
| `taskmesh.scheduling.workerPoolSize` | 375 | `t × d` — **placeholder**, computed from assumed N=20, d=15s (see below) |
| `taskmesh.scheduling.batchSize` | 500 | `headroom × t × queryInterval`, headroom≈2.5×, same assumptions |
| `taskmesh.scheduling.queryIntervalSeconds` | 10 | fallback/max-wait only — primary trigger is pool-drain, not this timer |
| `taskmesh.scheduling.queueCapacity` | 500 | ≈ one batch's worth |
| `taskmesh.scheduling.safetyNetPollSeconds` | 30 | unrelated to throughput math, workflow-derived default |

**These defaults are placeholders, not sourced numbers** — they're computed
from N=20 nodes and d=15s avg task duration, both assumptions, not values
taken from the origin project (which explicitly hasn't pinned N — tag `PRG`
in that project's `open-questions.md`). They exist so the profile module has
*something* sensible to ship rather than requiring every consumer to derive
their own from scratch — the formulas in this doc are what to recompute from
once real N/d numbers exist, at which point these defaults get updated (or a
consumer overrides them directly for their own deployment without waiting
for that).

## Packaging: one sub-project per profile

Rather than requiring a consumer to hand-pick and wire N interface
implementations correctly, each profile becomes a thin aggregator module —
a "starter," in the Spring Boot sense — that depends on `taskmesh-engine` +
whichever concrete implementation classes it selects (from `taskmesh-postgres`
etc.) and ships sensible tuned defaults (lease duration, poll cadence, pool
size, history level) as one importable unit:

```
taskmesh-profile-batch         → BATCH defaults
taskmesh-profile-highvolume    → HIGH_VOLUME defaults (this project's own consumer uses this one)
taskmesh-profile-reactive      → REACTIVE defaults
```

**Implementation classes are not duplicated per profile** — `LeaseClaimStrategy`
lives once, in `taskmesh-postgres`; different profile modules just configure
it differently (lease duration, etc.). A profile module is config + defaults,
not a fork of the code. A consumer depends on exactly one profile module to
get a coherent, tuned starting point, and can still override individual
interface implementations or config values if their actual workload doesn't
match any existing profile precisely — the profile is a default, not a lock-in.

See `module-layout.md` for where these fit in the overall dependency graph.
