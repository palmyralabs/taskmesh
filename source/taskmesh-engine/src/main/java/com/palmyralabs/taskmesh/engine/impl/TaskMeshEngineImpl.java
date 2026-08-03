package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.ActorType;
import com.palmyralabs.taskmesh.api.AuditEvent;
import com.palmyralabs.taskmesh.api.AuditEventSink;
import com.palmyralabs.taskmesh.api.BacklogPromoter;
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
import com.palmyralabs.taskmesh.api.TimeoutPolicy;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    public TaskMeshEngineImpl(NodeInfoProvider nodeInfoProvider, ProcessorRegistry processorRegistry,
                               List<String> taskTypes, ClaimStrategy claimStrategy, HeartbeatStrategy heartbeatStrategy,
                               TaskLedger taskLedger, BacklogPromoter backlogPromoter, FailureClassifier failureClassifier,
                               PauseController pauseController,
                               HistoryWriter historyWriter, AuditEventSink auditEventSink, IncidentStore incidentStore,
                               MetricsReporter metricsReporter, TimeoutPolicy timeoutPolicy,
                               PayloadProviderFactory payloadProviderFactory, SchedulingConfig schedulingConfig) {
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

        int regularCapacity = Math.max(1, schedulingConfig.getQueueCapacity() - SchedulingConfig.PRIORITY_QUEUE_CAPACITY);
        this.executionQueue = new FifoPriorityQueueImpl(SchedulingConfig.PRIORITY_QUEUE_CAPACITY, regularCapacity);
        this.workerPool = buildWorkerPool();
    }

    private ThreadPoolExecutor buildWorkerPool() {
        AtomicInteger threadCount = new AtomicInteger();
        int poolSize = schedulingConfig.getWorkerPoolSize();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                poolSize, poolSize, 60L, TimeUnit.SECONDS, executionQueue,
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
        log.info("taskmesh engine starting, nodeId={}, clusterPaused={}",
                nodeInfoProvider.nodeId(), pauseController.isPauseRequested());
        long intervalMillis = TimeUnit.SECONDS.toMillis(schedulingConfig.getQueryIntervalSeconds());
        pollerScheduler.scheduleWithFixedDelay(this::pollOnce, 0, intervalMillis, TimeUnit.MILLISECONDS);
        long backlogIntervalMillis = TimeUnit.SECONDS.toMillis(schedulingConfig.getBacklogIntervalSeconds());
        backlogScheduler.scheduleWithFixedDelay(this::promoteBacklogOnce, 0, backlogIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        log.info("taskmesh engine stopping, nodeId={}", nodeInfoProvider.nodeId());
        shutdownRequested = true;
        pollerScheduler.shutdownNow();
        backlogScheduler.shutdownNow();
        workerPool.shutdown();
        try {
            workerPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
        taskLedger.enqueue(taskRef);
        backlogPromoter.onEnqueue(taskRef, nodeInfoProvider.nodeId());
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
        taskLedger.enqueue(taskRef);
        // Same hook as enqueue() — note a throttling BacklogPromoter that defers
        // hot-row creation here also defers priority-path claimability, since
        // claim() operates on tm_in_flight; that tradeoff belongs to the
        // BacklogPromoter implementation, not this call site.
        backlogPromoter.onEnqueue(taskRef, nodeInfoProvider.nodeId());
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
        for (String taskType : taskTypes) {
            int available = Math.min(executionQueue.regularRemainingCapacity(), schedulingConfig.getBatchSize());
            if (available <= 0) {
                continue;
            }
            List<TaskRef> pending = taskLedger.pullPending(taskType, Instant.now(), available);
            for (TaskRef taskRef : pending) {
                submitFromPoller(taskRef);
            }
        }
    }

    /** Periodic hot-row promotion — a no-op call for NoopBacklogPromoter, real work for a throttling implementation. */
    private void promoteBacklogOnce() {
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
            try {
                attempt(taskRef);
            } finally {
                inFlightSet.remove(taskRef);
            }
        };
    }

    private void attempt(TaskRef taskRef) {
        // Paused: do not claim new work — leave it QUEUED for pickup on resume. This also
        // covers the enqueue()/execute() fast-path, which reaches here without the poller.
        if (pauseController.isPauseRequested()) {
            return;
        }
        // Phase 2 — Claim
        ClaimOutcome claimOutcome = claimStrategy.claim(taskRef);
        if (claimOutcome == ClaimOutcome.LOST) {
            return; // silent, normal — not logged above debug, per runtime-flow.md
        }
        metricsReporter.taskClaimed(taskRef.taskType());
        // Read fresh off the hot row right after claim, rather than an engine-local counter —
        // that wouldn't survive a JVM restart or agree across nodes. Already the current attempt's
        // own number (set at row creation, bumped on each requeue) — no further +1. See TaskLedger#attemptCount.
        int attemptNumber = taskLedger.attemptCount(taskRef);

        // Phase 3 — Pre-dispatch
        if (!processorRegistry.isRegistered(taskRef.taskType())) {
            // D7: unregistered task_type fails loudly, doesn't burn a retry attempt.
            incidentStore.open(new Incident(taskRef, "NO_PROCESSOR",
                    "No TaskProcessor registered for task type '" + taskRef.taskType() + "'", Instant.now()));
            claimStrategy.release(taskRef, ReleaseRequest.failedTerminal("NO_PROCESSOR", "No TaskProcessor registered"));
            historyWriter.record(new HistoryEvent(taskRef, attemptNumber, HistoryEventType.FAILED, "NO_PROCESSOR", Instant.now(), nodeInfoProvider.nodeId()));
            return;
        }
        TaskProcessor processor = processorRegistry.lookup(taskRef.taskType());
        PayloadProvider payloadProvider = payloadProviderFactory.getProvider(taskRef.taskType());

        Duration timeout = timeoutPolicy.timeoutFor(taskRef.taskType());
        Instant deadline = Instant.now().plus(timeout);
        DefaultTaskContext ctx = new DefaultTaskContext(taskRef, attemptNumber, payloadProvider, heartbeatStrategy, pauseController,
                () -> shutdownRequested);
        historyWriter.record(new HistoryEvent(taskRef, attemptNumber, HistoryEventType.STARTED, null, Instant.now(), nodeInfoProvider.nodeId()));

        // Phase 4 — Execution
        Instant startedAt = Instant.now();
        try {
            processor.process(ctx);

            if (!ctx.isOwnershipValid()) {
                // Phase 6 — lost ownership mid-execution: no release, audit-only history.
                historyWriter.record(new HistoryEvent(taskRef, attemptNumber, HistoryEventType.OWNERSHIP_LOST, null, Instant.now(), nodeInfoProvider.nodeId()));
                return;
            }
            if (Instant.now().isAfter(deadline)) {
                // Q-TIMEOUT: detection only, not preemption.
                incidentStore.open(new Incident(taskRef, "TIMEOUT_EXCEEDED",
                        "Task exceeded its declared timeout of " + timeout, Instant.now()));
            }

            // Phase 5a — Success
            claimStrategy.release(taskRef, ReleaseRequest.completed());
            historyWriter.record(new HistoryEvent(taskRef, attemptNumber, HistoryEventType.COMPLETED, null, Instant.now(), nodeInfoProvider.nodeId()));
            metricsReporter.taskCompleted(taskRef.taskType(), Duration.between(startedAt, Instant.now()));
        } catch (Exception failure) {
            if (!ctx.isOwnershipValid()) {
                historyWriter.record(new HistoryEvent(taskRef, attemptNumber, HistoryEventType.OWNERSHIP_LOST,
                        summarize(failure), Instant.now(), nodeInfoProvider.nodeId()));
                return;
            }
            handleFailure(taskRef, attemptNumber, failure);
        }
    }

    /** Phase 5b/5c — retryable or terminal failure. */
    private void handleFailure(TaskRef taskRef, int attemptNumber, Exception failure) {
        Classification classification = failureClassifier.classify(failure);
        ReleaseRequest request = classification.retryable()
                ? ReleaseRequest.requeue(classification.reasonCode(), summarize(failure))
                : ReleaseRequest.failedTerminal(classification.reasonCode(), summarize(failure));

        ReleaseOutcome outcome = claimStrategy.release(taskRef, request);

        switch (outcome) {
            case LOST_OWNERSHIP -> historyWriter.record(new HistoryEvent(
                    taskRef, attemptNumber, HistoryEventType.OWNERSHIP_LOST, summarize(failure), Instant.now(), nodeInfoProvider.nodeId()));
            case OK -> {
                if (classification.retryable()) {
                    historyWriter.record(new HistoryEvent(
                            taskRef, attemptNumber, HistoryEventType.RETRIED, classification.reasonCode(), Instant.now(), nodeInfoProvider.nodeId()));
                    metricsReporter.taskRetried(taskRef.taskType(), classification.reasonCode());
                } else {
                    openTerminalIncident(taskRef, attemptNumber, classification, failure);
                }
            }
            case EXHAUSTED -> openTerminalIncident(taskRef, attemptNumber, classification, failure);
        }
    }

    private void openTerminalIncident(TaskRef taskRef, int attemptNumber, Classification classification, Exception failure) {
        String reasonCode = classification.reasonCode();
        incidentStore.open(new Incident(taskRef, reasonCode, summarize(failure), Instant.now()));
        historyWriter.record(new HistoryEvent(taskRef, attemptNumber, HistoryEventType.FAILED, reasonCode, Instant.now(), nodeInfoProvider.nodeId()));
        metricsReporter.taskFailed(taskRef.taskType(), reasonCode);
    }

    private static String summarize(Throwable failure) {
        String message = failure.getMessage();
        return message != null ? message : failure.getClass().getSimpleName();
    }
}
