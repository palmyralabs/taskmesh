package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.AttemptCounters;
import com.palmyralabs.taskmesh.api.ActorType;
import com.palmyralabs.taskmesh.api.AuditEvent;
import com.palmyralabs.taskmesh.api.AuditEventSink;
import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.DuplicateEnqueueException;
import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.Classification;
import com.palmyralabs.taskmesh.api.FailureClassifier;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.HistoryEvent;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import com.palmyralabs.taskmesh.api.HistoryWriter;
import com.palmyralabs.taskmesh.api.Incident;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.MetricsReporter;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.PauseController;
import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.ProcessorRegistry;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.TaskLedger;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.api.TransactionRunner;
import com.palmyralabs.taskmesh.api.TerminalOutcome;
import com.palmyralabs.taskmesh.api.TerminalDisposition;
import com.palmyralabs.taskmesh.api.TimeoutPolicy;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The driving implementation (D8/module-layout.md) of {@link TaskMeshEngine}:
 * feeder, claim loop, dispatcher, worker pool. Wires the -api interfaces
 * together; has no idea what any TaskProcessor actually does, and never
 * branches on deployment topology (single-node vs multi-node is entirely a
 * ClaimStrategy/HeartbeatStrategy/TaskLedger implementation concern).
 *
 * <p>Construct via {@link TaskMeshEngine#builder()}, not this class's
 * constructor directly — the constructor is public only because {@code impl}
 * is a separate package from {@link TaskMeshEngine} itself (D-something:
 * "TaskMeshEngine as interface, TaskMeshEngineImpl as implementation, all
 * implementation classes under impl").
 *
 * <p>Owns three producer-facing entry points into the execution queue
 * (D25): {@link #enqueue}, the engine-internal poller ({@code submit}), and
 * {@link #execute}. See {@link FifoPriorityQueueImpl} for the queue itself
 * and {@link InFlightSet} for the cross-producer dedup guard (D26).
 */
@Slf4j
public class TaskMeshEngineImpl implements TaskMeshEngine {

    private final NodeInfoProvider nodeInfoProvider;
    private final ProcessorRegistry processorRegistry;
    private final List<String> taskTypes;
    private final ClaimStrategy claimStrategy;
    private final HeartbeatStrategy heartbeatStrategy;
    private final TaskLedger taskLedger;
    private final BacklogPromoter backlogPromoter;
    private final FailureClassifier failureClassifier;
    private final PauseController pauseController;
    private final HistoryWriter historyWriter;
    private final AuditEventSink auditEventSink;
    private final IncidentStore incidentStore;
    private final MetricsReporter metricsReporter;
    private final TimeoutPolicy timeoutPolicy;
    private final PayloadProviderFactory payloadProviderFactory;
    private final SchedulingConfig schedulingConfig;
    private final TransactionRunner transactionRunner;

    private final FifoPriorityQueueImpl executionQueue;
    private final ThreadPoolExecutor workerPool;
    private final InFlightSet inFlightSet = new InFlightSet();
    private final ScheduledExecutorService pollerScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "taskmesh-poller");
        thread.setDaemon(true);
        return thread;
    });
    private final ScheduledExecutorService backlogScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "taskmesh-backlog-promoter");
        thread.setDaemon(true);
        return thread;
    });

    // Guards workerPool's core/max resize pair (D30) so a shrink and a grow
    // racing each other can't leave core > max mid-flight.
    private final Object poolResizeLock = new Object();

    private volatile boolean shutdownRequested = false;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.STOPPED);

    /** Incremented as each queued task is picked up by a worker — the queue's consumption signal. */
    private final AtomicLong queueConsumed = new AtomicLong();
    /** When the poller runs next; policy lives in {@link PollCadence}, this class only obeys it. */
    private final PollCadence pollCadence;

    public TaskMeshEngineImpl(NodeInfoProvider nodeInfoProvider, ProcessorRegistry processorRegistry,
                               List<String> taskTypes, ClaimStrategy claimStrategy, HeartbeatStrategy heartbeatStrategy,
                               TaskLedger taskLedger, BacklogPromoter backlogPromoter, FailureClassifier failureClassifier,
                               PauseController pauseController,
                               HistoryWriter historyWriter, AuditEventSink auditEventSink, IncidentStore incidentStore,
                               MetricsReporter metricsReporter, TimeoutPolicy timeoutPolicy,
                               PayloadProviderFactory payloadProviderFactory, SchedulingConfig schedulingConfig,
                               TransactionRunner transactionRunner) {
        this.nodeInfoProvider = nodeInfoProvider;
        this.processorRegistry = processorRegistry;
        this.taskTypes = taskTypes;
        this.claimStrategy = claimStrategy;
        this.heartbeatStrategy = heartbeatStrategy;
        this.taskLedger = taskLedger;
        this.backlogPromoter = backlogPromoter;
        this.failureClassifier = failureClassifier;
        this.pauseController = pauseController;
        this.historyWriter = historyWriter;
        this.auditEventSink = auditEventSink;
        this.incidentStore = incidentStore;
        this.metricsReporter = metricsReporter;
        this.timeoutPolicy = timeoutPolicy;
        this.payloadProviderFactory = payloadProviderFactory;
        this.schedulingConfig = schedulingConfig;
        this.transactionRunner = transactionRunner;

        int regularCapacity = Math.max(1, schedulingConfig.getQueueCapacity() - SchedulingConfig.PRIORITY_QUEUE_CAPACITY);
        this.executionQueue = new FifoPriorityQueueImpl(SchedulingConfig.PRIORITY_QUEUE_CAPACITY, regularCapacity);
        this.pollCadence = new PollCadence(schedulingConfig, regularCapacity, nodeInfoProvider.nodeId());
        this.workerPool = buildWorkerPool();
    }

    private ThreadPoolExecutor buildWorkerPool() {
        AtomicInteger threadCount = new AtomicInteger();
        int ceiling = schedulingConfig.getWorkerPoolSize();
        int configuredInitial = schedulingConfig.getInitialPoolSize();
        // 0 means "start at the ceiling" — the original behavior, unchanged
        // for every consumer that never sets initialPoolSize. Clamped to
        // [1, ceiling], the same bound adjustPoolSize applies at runtime.
        int initial = configuredInitial <= 0 ? ceiling : Math.max(1, Math.min(configuredInitial, ceiling));
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                initial, ceiling, 60L, TimeUnit.SECONDS, executionQueue,
                runnable -> {
                    Thread thread = new Thread(runnable,
                            "taskmesh-" + nodeInfoProvider.nodeId() + "-worker-" + threadCount.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
        // Workers are the queue's only consumers, and enqueue()/execute() push
        // directly into the queue (bypassing executor.execute()) — they must
        // already be alive to pick anything up.
        executor.prestartAllCoreThreads();
        return executor;
    }

    /** Phase 0 — startup. */
    @Override
    public void start() {
        shutdownRequested = false;
        status.set(Status.RUNNING);
        // Before the log line below, which reads isPauseRequested(): implementations that poll a
        // backing store (JpaPauseController) only populate their cache once started.
        pauseController.start();
        log.info("taskmesh engine starting, nodeId={}, clusterPaused={}",
                nodeInfoProvider.nodeId(), pauseController.isPauseRequested());
        long intervalMillis = TimeUnit.SECONDS.toMillis(schedulingConfig.getQueryIntervalSeconds());
        if (schedulingConfig.isAdaptivePolling()) {
            // Self-rescheduling rather than scheduleWithFixedDelay, which cannot vary its delay.
            pollCadence.reset(queueConsumed.get(), System.nanoTime());
            pollerScheduler.schedule(this::adaptivePollCycle, 0, TimeUnit.MILLISECONDS);
        } else {
            pollerScheduler.scheduleWithFixedDelay(guarded("poller", this::pollOnce), 0, intervalMillis, TimeUnit.MILLISECONDS);
        }
        long backlogIntervalMillis = TimeUnit.SECONDS.toMillis(schedulingConfig.getBacklogIntervalSeconds());
        backlogScheduler.scheduleWithFixedDelay(guarded("backlog promoter", this::promoteBacklogOnce),
                0, backlogIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        log.info("taskmesh engine stopping, nodeId={}", nodeInfoProvider.nodeId());
        shutdownRequested = true;
        status.set(Status.DRAINING);
        pollerScheduler.shutdownNow();
        backlogScheduler.shutdownNow();
        workerPool.shutdown();
        try {
            workerPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pauseController.stop();
            status.set(Status.STOPPED);
        }
    }

    @Override
    public Status getStatus() {
        return status.get();
    }

    /**
     * Runtime worker-pool resize (D30) — bounded to {@code [1, the pool size the
     * engine was built with]}: callers can throttle down and recover, never scale
     * past the originally configured ceiling. Backpressure policy (when/why to
     * resize) is entirely the caller's concern; the engine just resizes.
     */
    @Override
    public void adjustPoolSize(int size) {
        int ceiling = schedulingConfig.getWorkerPoolSize();
        int bounded = Math.max(1, Math.min(size, ceiling));
        synchronized (poolResizeLock) {
            int current = workerPool.getCorePoolSize();
            if (bounded == current) {
                return;
            }
            if (bounded > current) {
                workerPool.setMaximumPoolSize(bounded);
                workerPool.setCorePoolSize(bounded);
            } else {
                workerPool.setCorePoolSize(bounded);
                workerPool.setMaximumPoolSize(bounded);
            }
            log.info("taskmesh engine worker pool resized {} -> {}, nodeId={}", current, bounded, nodeInfoProvider.nodeId());
        }
    }

    @Override
    public int currentPoolSize() {
        return workerPool.getCorePoolSize();
    }

    /** Operator-triggered pause — the one case AuditEventSink is written to (never for routine transitions). */
    @Override
    public void pause(String performedBy) {
        pauseController.requestPause();
        auditEventSink.record(new AuditEvent("PAUSE_REQUESTED", null, ActorType.APPLICATION_USER,
                performedBy, "Cluster pause requested", Instant.now()));
    }

    @Override
    public void resume(String performedBy) {
        pauseController.resume();
        auditEventSink.record(new AuditEvent("RESUME_REQUESTED", null, ActorType.APPLICATION_USER,
                performedBy, "Cluster resume requested", Instant.now()));
    }

    @Override
    public void rerun(TaskRef taskRef, String performedBy) {
        taskLedger.rerun(taskRef);
        incidentStore.resolve(taskRef, performedBy);
        auditEventSink.record(new AuditEvent("TASK_RERUN", taskRef, ActorType.APPLICATION_USER,
                performedBy, "Task manually requeued for rerun", Instant.now()));
        // Best-effort immediate pickup on this node (same fast-path as execute()); the poller is the safety net.
        if (inFlightSet.add(taskRef)) {
            boolean pushed = executionQueue.offerRegular(dispatchTask(taskRef));
            if (!pushed) {
                inFlightSet.remove(taskRef);
            }
        }
    }

    @Override
    public void resolveIncident(TaskRef taskRef, String performedBy) {
        incidentStore.resolve(taskRef, performedBy);
        auditEventSink.record(new AuditEvent("INCIDENT_RESOLVED", taskRef, ActorType.APPLICATION_USER,
                performedBy, "Incident manually resolved", Instant.now()));
    }

    /**
     * Plain enqueue — external API. Persists via {@link TaskLedger#enqueue}
     * unconditionally; additionally pushes into the execution queue's
     * regularArray only if it isn't already past half capacity (D25,
     * crownshaft's {@code isHalfFull()}/{@code canEnqueue()} pattern). If
     * the gate fails, or another producer already has this ref in flight,
     * the row is still durable — the poller picks it up on its next cycle.
     */
    @Override
    public void enqueue(TaskRef taskRef) {
        persist(taskRef);
        if (executionQueue.isRegularHalfFull()) {
            return;
        }
        if (!inFlightSet.add(taskRef)) {
            return;
        }
        boolean pushed = executionQueue.offerRegular(dispatchTask(taskRef));
        if (!pushed) {
            inFlightSet.remove(taskRef);
        }
    }

    /**
     * Priority entry point — external API. Persists (same durability
     * guarantee as {@link #enqueue}) and pushes directly into the execution
     * queue's priorityArray, waiting up to {@code priorityOfferTimeoutMillis}
     * for a slot. On timeout, the immediate push is rejected — not the
     * whole call — the row is already durable, so nothing is lost; the
     * poller remains the safety net (D25).
     */
    @Override
    public void execute(TaskRef taskRef) {
        // Same hook as enqueue() — note a throttling BacklogPromoter that defers
        // hot-row creation here also defers priority-path claimability, since
        // claim() operates on tm_in_flight; that tradeoff belongs to the
        // BacklogPromoter implementation, not this call site.
        persist(taskRef);
        if (!inFlightSet.add(taskRef)) {
            return;
        }
        boolean pushed;
        try {
            pushed = executionQueue.offerPriority(dispatchTask(taskRef),
                    schedulingConfig.getPriorityOfferTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pushed = false;
        }
        if (!pushed) {
            inFlightSet.remove(taskRef);
            log.warn("{} priorityArray full — falling back to poller pickup", taskRef);
        }
    }

    /** The poller — engine-owned timer now drives TaskLedger.pullPending (D22), not a self-scheduled SchedulingStrategy. */
    private void pollOnce() {
        if (pauseController.isPauseRequested()) {
            return; // paused — the poller stops claiming new work (in-flight tasks still finish).
        }
        // Pagination is PollCadence's call, not batchSize's: it is bounded to a hard
        // 16..512 so a misconfigured batch can neither trickle nor hold a huge query open.
        int available = pollCadence.pageSize(executionQueue.regularRemainingCapacity(), schedulingConfig.getBatchSize());
        if (available <= 0) {
            return;
        }
        // One query for the whole node, ordered by next_retry_at — not one per task type. Querying
        // per type made the ordering FIFO only *within* a type, so a task queued an hour ago waited
        // behind one queued a second ago of a different type; worse, the queue capacity is shared
        // and consumed in taskTypes order, so the first type could take the entire cycle and starve
        // the rest. Registration order is not a priority scheme.
        List<TaskRef> pending = taskLedger.pullPending(Instant.now(), available);
        for (TaskRef taskRef : pending) {
            // The type filter moved off the query and to here. Without it a node could claim a type
            // it has no processor for, and the D7 path would open a NO_PROCESSOR incident and fail
            // the task terminally — destroying work another node was equipped to run. Skipping is
            // exactly what the old per-type query did implicitly: a type this node cannot handle is
            // simply never picked up by it.
            if (!processorRegistry.isRegistered(taskRef.taskType())) {
                continue;
            }
            submitFromPoller(taskRef);
        }
    }

    /**
     * Wraps a scheduled task so a throw cannot silently retire it.
     *
     * <p>{@code scheduleWithFixedDelay} cancels all future executions the first time its task
     * throws, and the thread factory installs no handler — so a single transient DB error would
     * stop this node polling or promoting for the life of the JVM, with nothing logged and the node
     * still looking healthy. Observed for real: two of three nodes' promoters died on one
     * duplicate-key collision and never ran again, and the only symptom was a slow queue.
     */
    private Runnable guarded(String what, Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Exception e) {
                // Swallowed deliberately — the next cycle is a fresh attempt, and losing the
                // schedule is far worse than losing one cycle. Loud, per runtime-flow.md: whatever
                // made this fail is a real fault, not routine contention.
                log.error("taskmesh {} cycle failed on node {}; the schedule continues and will retry next cycle",
                        what, nodeInfoProvider.nodeId(), e);
            } catch (Error e) {
                log.error("taskmesh {} cycle hit a JVM-level error on node {}; the schedule is being abandoned",
                        what, nodeInfoProvider.nodeId(), e);
                throw e;   // OOM/NoClassDefFound is not something a retry fixes
            }
        };
    }

    /**
     * One adaptive poll, then re-arms itself at a delay derived from how fast work is actually
     * leaving the queue. {@code guarded} keeps a failed cycle from killing the loop, exactly as
     * the fixed-interval path relies on it.
     */
    private void adaptivePollCycle() {
        guarded("poller", this::pollOnce).run();
        long delay = pollCadence.nextDelayMillis(queueConsumed.get(), System.nanoTime());
        if (shutdownRequested) {
            return;
        }
        try {
            pollerScheduler.schedule(this::adaptivePollCycle, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException stopping) {
            // stop() shut the scheduler between the check above and this submit — nothing to do.
        }
    }

    /** Periodic hot-row promotion — a no-op call for NoopBacklogPromoter, real work for a throttling implementation. */
    private void promoteBacklogOnce() {
        // Paused: do not promote either. Promotion is upstream of claiming, so letting it run while
        // the cluster is paused would keep filling tm_in_flight from a backlog nobody is draining —
        // for a throttling promoter that is the one thing its in-flight window exists to prevent,
        // and the window would be full of stale reservations by the time work resumes. Mirrors the
        // check pollOnce() opens with; a task left in tm_task is picked up on the next cycle after
        // resume, exactly like one left QUEUED in tm_in_flight.
        if (pauseController.isPauseRequested()) {
            return;
        }
        for (String taskType : taskTypes) {
            backlogPromoter.promote(taskType, Instant.now(), schedulingConfig.getBacklogBatchSize());
        }
    }

    private void submitFromPoller(TaskRef taskRef) {
        if (!inFlightSet.add(taskRef)) {
            return;
        }
        boolean pushed = executionQueue.offerFromPoller(dispatchTask(taskRef));
        if (!pushed) {
            inFlightSet.remove(taskRef);
        }
    }

    private Runnable dispatchTask(TaskRef taskRef) {
        return () -> {
            // Counted here, at pickup rather than completion: this is the queue's consumption
            // rate, which is what the poller has to keep ahead of.
            queueConsumed.incrementAndGet();
            try {
                attempt(taskRef);
            } finally {
                inFlightSet.remove(taskRef);
            }
        };
    }

    /** Reason code for a task terminated at claim time because its budget was already spent (D33). */
    private static final String RETRY_BUDGET_EXHAUSTED = "RETRY_BUDGET_EXHAUSTED";

    private void attempt(TaskRef taskRef) {
        // Paused: do not claim new work — leave it QUEUED for pickup on resume. This also
        // covers the enqueue()/execute() fast-path, which reaches here without the poller.
        if (pauseController.isPauseRequested()) {
            return;
        }
        // Phase 2 — Claim. ClaimStrategy opens no transaction of its own; the boundary is ours,
        // which is what lets a claim that turns out to be terminal (D33's exhausted-at-claim path)
        // commit its terminal write and the consumer's onTerminal together.
        ClaimOutcome claimOutcome = claimAndNotify(taskRef);
        if (claimOutcome == ClaimOutcome.LOST) {
            return; // silent, normal — not logged above debug, per runtime-flow.md
        }
        // Read fresh off the row right after claim, rather than an engine-local counter — that
        // wouldn't survive a JVM restart or agree across nodes. Already the current execution's
        // own (attempt_count, retry_count) — no further +1. See TaskLedger#counters.
        AttemptCounters counters = taskLedger.counters(taskRef);

        if (claimOutcome == ClaimOutcome.EXHAUSTED) {
            // D33: retry budget is spent at claim time, so a task whose previous execution died
            // without ever releasing is terminated here rather than reclaimed forever. The ledger
            // has already written the cold FAILED snapshot and deleted the hot row — there is no
            // claim left to release — so all that remains is the same bookkeeping a release-side
            // exhaustion does (D19 symmetry). Never silent: this is exactly the failure that used
            // to disappear.
            log.warn("Task {} terminated at claim: retry budget exhausted with no reported outcome "
                    + "(attempt {}, retry {}) — the previous execution's node died or lost its lease",
                    taskRef, counters.attemptCount(), counters.retryCount());
            incidentStore.open(new Incident(taskRef, RETRY_BUDGET_EXHAUSTED,
                    "Retry budget exhausted with no reported outcome — the previous execution's node "
                            + "lost its lease or died without releasing", Instant.now()));
            recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(),
                    HistoryEventType.FAILED, RETRY_BUDGET_EXHAUSTED, Instant.now(), nodeInfoProvider.nodeId()));
            metricsReporter.taskFailed(taskRef.taskType(), RETRY_BUDGET_EXHAUSTED);
            return;
        }
        metricsReporter.taskClaimed(taskRef.taskType());

        // Phase 3 — Pre-dispatch
        if (!processorRegistry.isRegistered(taskRef.taskType())) {
            // D7: unregistered task_type fails loudly, doesn't burn a retry attempt.
            incidentStore.open(new Incident(taskRef, "NO_PROCESSOR",
                    "No TaskProcessor registered for task type '" + taskRef.taskType() + "'", Instant.now()));
            claimStrategy.release(taskRef, ReleaseRequest.failedTerminal("NO_PROCESSOR", "No TaskProcessor registered"));
            recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.FAILED, "NO_PROCESSOR", Instant.now(), nodeInfoProvider.nodeId()));
            return;
        }
        TaskProcessor processor = processorRegistry.lookup(taskRef.taskType());
        PayloadProvider payloadProvider = payloadProviderFactory.getProvider(taskRef.taskType());

        Duration timeout = timeoutPolicy.timeoutFor(taskRef.taskType());
        Instant deadline = Instant.now().plus(timeout);
        DefaultTaskContext ctx = new DefaultTaskContext(taskRef, counters, payloadProvider, heartbeatStrategy, pauseController,
                () -> shutdownRequested, transactionRunner);
        recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.STARTED, null, Instant.now(), nodeInfoProvider.nodeId()));

        // Phase 4 — Execution
        Instant startedAt = Instant.now();
        try {
            // Its own short transaction, before any work is done. A consumer projection learns
            // here that the task is alive — including that an operator re-ran it, which nothing
            // else in the engine would tell it (see TaskProcessor#onStart). A throw travels the
            // ordinary classified-failure path below, so a broken hook is retried and bounded
            // rather than silently skipped.
            transactionRunner.call(() -> {
                processor.onStart(ctx);
                return null;
            });

            processor.process(ctx);

            if (!ctx.isOwnershipValid()) {
                // Phase 6 — lost ownership mid-execution: no release, audit-only history.
                recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.OWNERSHIP_LOST, null, Instant.now(), nodeInfoProvider.nodeId()));
                return;
            }
            if (Instant.now().isAfter(deadline)) {
                // Q-TIMEOUT: detection only, not preemption.
                incidentStore.open(new Incident(taskRef, "TIMEOUT_EXCEEDED",
                        "Task exceeded its declared timeout of " + timeout, Instant.now()));
            }

            // Phase 5a — Success
            releaseAndNotify(taskRef, ReleaseRequest.completed(), counters, processor, ctx);
            recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.COMPLETED, null, Instant.now(), nodeInfoProvider.nodeId()));
            metricsReporter.taskCompleted(taskRef.taskType(), Duration.between(startedAt, Instant.now()));
        } catch (Exception failure) {
            if (!ctx.isOwnershipValid()) {
                recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.OWNERSHIP_LOST,
                        summarize(failure), Instant.now(), nodeInfoProvider.nodeId()));
                return;
            }
            handleFailure(taskRef, counters, failure, processor, ctx);
        } catch (Error error) {
            // An Error (OOM, AssertionError, NoClassDefFoundError, ...) is a JVM/environment-level
            // failure, not classifiable task data — never routed through FailureClassifier (its
            // contract is Exception only) and never burns a retry attempt against a bug that will
            // just re-throw identically. Released immediately as terminal/incident-worthy, matching
            // how NO_PROCESSOR is handled, rather than silently stranding the claim forever (the gap
            // this whole catch clause exists to close — see TaskMeshEngineFailureModesTest).
            if (!ctx.isOwnershipValid()) {
                recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.OWNERSHIP_LOST,
                        summarize(error), Instant.now(), nodeInfoProvider.nodeId()));
                throw error;
            }
            incidentStore.open(new Incident(taskRef, "PROCESSOR_ERROR", summarize(error), Instant.now()));
            releaseAndNotify(taskRef, ReleaseRequest.failedTerminal("PROCESSOR_ERROR", summarize(error)),
                    counters, processor, ctx);
            recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.FAILED, "PROCESSOR_ERROR",
                    Instant.now(), nodeInfoProvider.nodeId()));
            metricsReporter.taskFailed(taskRef.taskType(), "PROCESSOR_ERROR");
            throw error; // cleanup done — don't swallow it; let it still surface via the worker thread's own error path
        }
    }

    /**
     * The one place a release happens, so the one place terminality is noticed. Both the ledger's
     * write and the consumer's {@link TaskProcessor#onTerminal} run inside a single transaction
     * boundary opened here: the ledger's own transaction joins this one rather than committing
     * separately, so a consumer projection and the queue's terminal state cannot diverge, and a
     * throwing callback takes the terminal transition down with it. Keeping the boundary here
     * rather than pushing a callback down into every {@code ClaimStrategy} leaves that interface —
     * a public extension seam — untouched, and the ledgers with nothing to know about consumers.
     *
     * <p>Deliberately <b>not</b> wrapped in a try/catch, unlike {@link #recordHistory}: history is
     * audit-only and must never affect control flow, whereas vetoing a terminal transition is
     * exactly what {@link TaskProcessor#onTerminal} is documented to be able to do.
     *
     * <p>The payload is whatever the execution already loaded, never a fresh fetch — a terminal
     * reached with no execution behind it has none, and {@code TypedTaskProcessor} passes null
     * through rather than rejecting it.
     */
    /**
     * The durable half of {@code enqueue}/{@code execute}: the cold/hot pair, written in **one**
     * transaction. The two steps are always called together — the ledger writes what it owns and
     * the {@link BacklogPromoter} decides when the hot row appears — so giving them one boundary
     * halves the commit count on the enqueue path, and matters most at the tier where the ledger
     * writes only the cold row and the promoter always writes the hot one immediately.
     *
     * <p>A lost enqueue race arrives as {@link DuplicateEnqueueException} rather than being
     * swallowed inside the ledger, because a swallow only works while the failed insert is
     * isolated: inside this boundary the violation has already marked the transaction
     * rollback-only. Losing it still isn't an error — {@code enqueue} is idempotent and the
     * winner's rows are exactly what this call wanted — so it is reported as success.
     */
    private void persist(TaskRef taskRef) {
        try {
            transactionRunner.call(() -> {
                taskLedger.enqueue(taskRef);
                backlogPromoter.onEnqueue(taskRef, nodeInfoProvider.nodeId());
                return null;
            });
        } catch (DuplicateEnqueueException e) {
            log.debug("Concurrent enqueue of {} lost the race; the other caller's rows stand", taskRef);
        }
    }

    /**
     * The claim-time twin of {@link #releaseAndNotify}. D33 lets a claim terminate a task rather
     * than admit it — the case {@code release()} structurally cannot see, since the node whose
     * execution died never reaches a release — so that transition needs the same boundary and the
     * same notification as any other terminal one.
     */
    private ClaimOutcome claimAndNotify(TaskRef taskRef) {
        TaskProcessor processor = processorRegistry.isRegistered(taskRef.taskType())
                ? processorRegistry.lookup(taskRef.taskType()) : null;
        return transactionRunner.call(() -> {
            ClaimOutcome outcome = claimStrategy.claim(taskRef);
            if (outcome == ClaimOutcome.EXHAUSTED && processor != null) {
                // Read before the terminal write's row deletion is visible to us, and with no
                // payload: no execution ran, so none was ever loaded. TypedTaskProcessor passes
                // that null through rather than rejecting it.
                processor.onTerminal(new DefaultTerminalContext(taskRef, taskLedger.counters(taskRef), null,
                        TerminalOutcome.FAILED_EXHAUSTED, RETRY_BUDGET_EXHAUSTED,
                        "Retry budget exhausted with no reported outcome — the previous execution's "
                                + "node lost its lease or died without releasing",
                        nodeInfoProvider.nodeId(), Instant.now()));
            }
            return outcome;
        });
    }

    private ReleaseOutcome releaseAndNotify(TaskRef taskRef, ReleaseRequest request, AttemptCounters counters,
                                             TaskProcessor processor, DefaultTaskContext ctx) {
        return transactionRunner.call(() -> {
            ReleaseOutcome outcome = claimStrategy.release(taskRef, request);
            TerminalOutcome terminal = terminalOutcomeOf(request.disposition(), outcome);
            if (terminal != null && processor != null) {
                processor.onTerminal(new DefaultTerminalContext(taskRef, counters,
                        ctx == null ? null : ctx.payloadIfLoaded(), terminal,
                        request.reasonCode(), request.errorSummary(),
                        nodeInfoProvider.nodeId(), Instant.now()));
            }
            return outcome;
        });
    }

    /**
     * Which terminal outcome this release produced, or {@code null} if it produced none. A
     * within-budget {@code RETRY} and a {@code YIELD} are not terminal; neither is anything that
     * lost ownership, since that node wrote nothing at all.
     */
    private static TerminalOutcome terminalOutcomeOf(TerminalDisposition disposition, ReleaseOutcome outcome) {
        if (outcome == ReleaseOutcome.LOST_OWNERSHIP) {
            return null;
        }
        if (outcome == ReleaseOutcome.EXHAUSTED) {
            return TerminalOutcome.FAILED_EXHAUSTED;   // a RETRY the ledger escalated (D19)
        }
        return switch (disposition) {
            case COMPLETED -> TerminalOutcome.COMPLETED;
            case FAILED_TERMINAL -> TerminalOutcome.FAILED_NONRETRYABLE;
            case RETRY, YIELD -> null;
        };
    }

    /**
     * Every {@code historyWriter.record(...)} call in this class goes through here rather than
     * calling {@code historyWriter} directly. {@link HistoryWriter} is documented as audit-only
     * ("must never affect control flow") but nothing previously enforced that on the write side —
     * a failing write (e.g. the STARTED write, which used to run before process() even started)
     * could leak a claim or abort a release that had already succeeded. Logged loudly (silence is
     * a bug) but never rethrown.
     */
    private void recordHistory(HistoryEvent event) {
        try {
            historyWriter.record(event);
        } catch (Exception e) {
            log.error("HistoryWriter failed to record {} for {}", event.type(), event.taskRef(), e);
        }
    }

    /** Phase 5b/5c — retryable or terminal failure. */
    private void handleFailure(TaskRef taskRef, AttemptCounters counters, Exception failure,
                                TaskProcessor processor, DefaultTaskContext ctx) {
        Classification classification = failureClassifier.classify(failure);
        ReleaseRequest request = classification.retryable()
                ? ReleaseRequest.retry(classification.reasonCode(), summarize(failure))
                : ReleaseRequest.failedTerminal(classification.reasonCode(), summarize(failure));

        ReleaseOutcome outcome = releaseAndNotify(taskRef, request, counters, processor, ctx);

        switch (outcome) {
            case LOST_OWNERSHIP -> recordHistory(new HistoryEvent(
                    taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.OWNERSHIP_LOST, summarize(failure), Instant.now(), nodeInfoProvider.nodeId()));
            case OK -> {
                if (classification.retryable()) {
                    recordHistory(new HistoryEvent(
                            taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.RETRIED, classification.reasonCode(), Instant.now(), nodeInfoProvider.nodeId()));
                    metricsReporter.taskRetried(taskRef.taskType(), classification.reasonCode());
                } else {
                    openTerminalIncident(taskRef, counters, classification, failure);
                }
            }
            case EXHAUSTED -> openTerminalIncident(taskRef, counters, classification, failure);
        }
    }

    private void openTerminalIncident(TaskRef taskRef, AttemptCounters counters, Classification classification, Exception failure) {
        String reasonCode = classification.reasonCode();
        incidentStore.open(new Incident(taskRef, reasonCode, summarize(failure), Instant.now()));
        recordHistory(new HistoryEvent(taskRef, counters.attemptCount(), counters.retryCount(), HistoryEventType.FAILED, reasonCode, Instant.now(), nodeInfoProvider.nodeId()));
        metricsReporter.taskFailed(taskRef.taskType(), reasonCode);
    }

    private static String summarize(Throwable failure) {
        String message = failure.getMessage();
        return message != null ? message : failure.getClass().getSimpleName();
    }
}
