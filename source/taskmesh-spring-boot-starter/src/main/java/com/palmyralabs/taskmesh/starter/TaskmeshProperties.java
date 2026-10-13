package com.palmyralabs.taskmesh.starter;

import com.palmyralabs.taskmesh.api.Jitter;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Generic taskmesh configuration — {@code taskmesh.*} — consumed by
 * {@link TaskmeshEngineAutoConfiguration} and available for manual use with
 * {@link TaskmeshProfileBuilder}. Not task-type/consumer specific (moved
 * out of the migration toolkit's own {@code TaskmeshMigrationProperties},
 * D29-successor decision — see decisions.md).
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "taskmesh")
public class TaskmeshProperties {

    /** Selects implementation classes (ClaimStrategy/HeartbeatStrategy/HistoryWriter) — see {@link WorkloadProfile}. */
    private WorkloadProfile profile = WorkloadProfile.HIGH_VOLUME;

    private long leaseSeconds = 300;
    private long reclaimSeconds = 60;
    private long affinitySeconds = 150;
    private long claimTimeoutSeconds = 300;
    /** How long a freshly enqueued QUEUED row stays reserved to the enqueuing node before another node may steal it. */
    private long reservationSeconds = 60;
    private int maxRetries = 10;
    private long backoffBaseMinutes = 2;
    private long backoffCapMinutes = 60;
    private Jitter backoffJitter = Jitter.NONE;

    private int workerPoolSize = 8;

    /**
     * Core pool size the worker pool starts at, before any runtime
     * {@code adjustPoolSize} call — see {@code SchedulingConfig.initialPoolSize}.
     * Defaults to 2 here (unlike the engine-level default of 0/"start at
     * ceiling"): a Spring Boot consumer of this starter is expected to pair
     * this with a backpressure policy that grows the pool as real load and
     * latency justify it, rather than every node prestarting its full
     * ceiling on day one regardless of whether there is any work yet.
     * Bounded to {@code [1, workerPoolSize]}.
     */
    private int initialPoolSize = 2;

    private int batchSize = 500;
    private int queryIntervalSeconds = 10;

    /**
     * Depth of the in-memory execution queue, including the fixed 5-slot priority lane —
     * so the regular lane is this minus 5, and that is what caps one poller cycle
     * ({@code min(regularRemainingCapacity, batchSize)}), making {@link #batchSize}'s
     * default of 500 unreachable until this is raised past it.
     *
     * <p>Size it to cover a full poll interval of work, or workers idle waiting for the
     * next cycle — the same rate x interval rule the in-flight watermarks use:
     *
     * <pre>
     *   queueCapacity &gt;= workerPoolSize * (queryIntervalSeconds / secondsPerExecution)
     * </pre>
     *
     * At 8 workers, a 10s interval and ~113ms per execution that is ~708, against the
     * default 100 — which is why a deployment leaving {@code queryIntervalSeconds} at its
     * default spends most of every cycle with idle workers. Lower the interval, raise this,
     * or both.
     */
    private int queueCapacity = 100;

    /**
     * Derive the poll cadence from the observed drain rate instead of the fixed
     * {@link #queryIntervalSeconds} timer. See SchedulingConfig#adaptivePolling: a fixed
     * interval has to be sized against workerPoolSize/perExecutionTime, which is workload
     * dependent and unknowable up front. {@link #queryIntervalSeconds} still acts as the
     * ceiling on the adaptive cadence.
     */
    private boolean adaptivePolling = true;

    /** Floor on the adaptive poll cadence, in milliseconds. */
    private long pollMinMillis = 50;

    /** Ceiling on the adaptive poll cadence. 0 means "use queryIntervalSeconds". */
    private long pollMaxMillis = 0;

    /**
     * How hot {@code tm_in_flight} rows are created relative to the cold
     * {@code tm_task} row — see {@link BacklogPromotion}. Only taskmesh-max
     * offers a choice; taskmesh-simple/-pro create the pair eagerly inside
     * {@code enqueue()} itself and ignore this.
     */
    private BacklogPromotion backlogPromotion = BacklogPromotion.EAGER;

    /**
     * {@code THROTTLED} promotion only — top {@code tm_in_flight} back up once it
     * falls below this, and stop at {@link #inFlightHighWatermark}. The gap between
     * the two is hysteresis: it stops a busy queue being re-topped by a handful of
     * rows on every cycle.
     *
     * <h4>Sizing these — they are not free parameters</h4>
     * A refill leaves the level at {@code high}, and the next promotion cycle is up
     * to {@link #backlogIntervalSeconds} ({@code T}) away. So for a node draining
     * {@code R} tasks/second:
     *
     * <pre>
     *   inFlightLowWatermark  &gt;= R * T        // trigger with a full interval still in hand
     *   inFlightHighWatermark &gt;= low + R * T  // a refill adds an interval's worth
     *   backlogBatchSize      &gt;= high - low   // ...and one cycle can actually deliver it
     * </pre>
     *
     * Undersize the low mark and the window empties before the next cycle can refill
     * it: workers idle against a backlog of millions, and the queue's throughput
     * collapses to {@code (high - low) / T} regardless of how many workers are
     * configured. That failure is silent — nothing errors, the drain is just slow.
     *
     * <p>The defaults below assume {@code R = 300} tasks/second and {@code T = 5s}:
     * {@code 300 * 5 = 1500} low, {@code 3000} high, {@code 1500} batch. Measure your
     * own {@code R} and re-derive; these numbers are a worked example, not a
     * universal default.
     */
    private int inFlightLowWatermark = 1500;
    private int inFlightHighWatermark = 3000;

    /**
     * Rows promoted per task type per cycle — the cap on one promotion transaction.
     * Must cover {@code inFlightHighWatermark - inFlightLowWatermark} or a refill
     * takes several cycles and the watermark sizing above stops holding.
     */
    /**
     * Re-derive the in-flight window every promotion cycle from what the node is actually
     * consuming, instead of holding {@link #inFlightLowWatermark}/{@link #inFlightHighWatermark}
     * fixed. When on, those two become the floor and ceiling of the derived window rather than the
     * window itself, and seed it until two consumption samples exist. THROTTLED promotion only.
     */
    private boolean adaptiveInFlightWindow = true;

    private int backlogBatchSize = 1500;
    private int backlogIntervalSeconds = 5;

    /** BATCH profile only ({@code ExplicitHeartbeatStrategy}) — gap since the last checkpoint before an incident is opened. */
    private long missedHeartbeatMinutes = 10;

    /** Blank (default) falls back to taskmesh's DefaultNodeInfoProvider (hostname+PID). */
    private String nodeId = "";

    /**
     * Per-instance participation switch — one of three independent layers of
     * taskmesh control (the other two are per-node dynamic pause/resume and
     * the cluster-wide {@code tm_control_flag}, both read at runtime via
     * {@code PauseController}; this one is read once, at startup). When
     * {@code false}, the {@code TaskMeshEngine} bean is still built and
     * wired — {@code enqueue}/{@code execute}/{@code pause}/{@code rerun} all
     * still work — but {@code engine.start()} is never called, so this node's
     * feeder, claim loop and worker pool never run. It will not poll, claim,
     * or execute any task, including ones enqueued from this same JVM.
     *
     * <p>Exists for developers pointing at a shared database: without it,
     * every node with the app running competes to claim and execute every
     * other node's queued work. Set {@code taskmesh.enabled: false} in
     * {@code application.yml} (or a profile) on any node that should observe
     * or enqueue but never execute. Renamed from {@code auto-start} —
     * "enabled" reads correctly next to the two other control layers, and
     * "auto-start" implied a boot-time nicety rather than a hard participation
     * gate.
     */
    private boolean enabled = true;
}
