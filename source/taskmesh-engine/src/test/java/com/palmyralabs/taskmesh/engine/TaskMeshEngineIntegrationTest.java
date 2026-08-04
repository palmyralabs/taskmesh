package com.palmyralabs.taskmesh.engine;

import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.api.TypedTaskProcessor;
import com.palmyralabs.taskmesh.engine.impl.SchedulingConfig;
import com.palmyralabs.taskmesh.testkit.FixedDelayRetryPolicy;
import com.palmyralabs.taskmesh.testkit.InMemoryAuditEventSink;
import com.palmyralabs.taskmesh.testkit.InMemoryBacklogPromoter;
import com.palmyralabs.taskmesh.testkit.InMemoryClaimStrategy;
import com.palmyralabs.taskmesh.testkit.InMemoryHeartbeatStrategy;
import com.palmyralabs.taskmesh.testkit.InMemoryHistoryWriter;
import com.palmyralabs.taskmesh.testkit.InMemoryIncidentStore;
import com.palmyralabs.taskmesh.testkit.InMemoryMetricsReporter;
import com.palmyralabs.taskmesh.testkit.InMemoryPauseController;
import com.palmyralabs.taskmesh.testkit.InMemoryPayloadStore;
import com.palmyralabs.taskmesh.testkit.InMemoryTaskLedger;
import com.palmyralabs.taskmesh.testkit.InMemoryTaskStore;
import com.palmyralabs.taskmesh.testkit.SingleProviderPayloadProviderFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end: TaskMeshEngine wired entirely against taskmesh-testkit fakes,
 * no real infra. Exercises enqueue()'s direct push path and, for retries,
 * the engine-owned poller (D22/D25).
 */
class TaskMeshEngineIntegrationTest {

    record GreetingPayload(String name) {
    }

    @Test
    void completesASimpleTaskEndToEnd() throws Exception {
        NodeInfoProvider nodeInfoProvider = () -> "node-1";
        InMemoryTaskStore store = new InMemoryTaskStore();
        InMemoryClaimStrategy claimStrategy = new InMemoryClaimStrategy(
                store, nodeInfoProvider, new FixedDelayRetryPolicy(3, Duration.ofMillis(10)));
        InMemoryHeartbeatStrategy heartbeatStrategy = new InMemoryHeartbeatStrategy(claimStrategy);
        InMemoryTaskLedger taskLedger = new InMemoryTaskLedger(store);
        InMemoryHistoryWriter historyWriter = new InMemoryHistoryWriter();
        InMemoryAuditEventSink auditEventSink = new InMemoryAuditEventSink();
        InMemoryIncidentStore incidentStore = new InMemoryIncidentStore();
        InMemoryMetricsReporter metricsReporter = new InMemoryMetricsReporter();
        InMemoryPauseController pauseController = new InMemoryPauseController();
        InMemoryPayloadStore payloadStore = new InMemoryPayloadStore();

        CountDownLatch completed = new CountDownLatch(1);

        TaskProcessor processor = new TypedTaskProcessor<GreetingPayload>() {
            @Override
            public String taskType() {
                return "GREETING";
            }

            @Override
            protected Class<GreetingPayload> payloadType() {
                return GreetingPayload.class;
            }

            @Override
            protected void process(TaskContext ctx, GreetingPayload payload) {
                ctx.checkpoint();
                completed.countDown();
            }
        };

        TaskMeshEngine engine = TaskMeshEngine.builder()
                .nodeInfoProvider(nodeInfoProvider)
                .processors(List.of(processor))
                .claimStrategy(claimStrategy)
                .heartbeatStrategy(heartbeatStrategy)
                .taskLedger(taskLedger)
                .backlogPromoter(new InMemoryBacklogPromoter())
                .pauseController(pauseController)
                .historyWriter(historyWriter)
                .auditEventSink(auditEventSink)
                .incidentStore(incidentStore)
                .metricsReporter(metricsReporter)
                .payloadProviderFactory(new SingleProviderPayloadProviderFactory(payloadStore))
                .schedulingConfig(SchedulingConfig.builder().queryIntervalSeconds(1).build())
                .build();

        TaskRef taskRef = new TaskRef("GREETING", "t1");
        payloadStore.store(taskRef, new GreetingPayload("Ada"));

        engine.start();
        try {
            engine.enqueue(taskRef); // direct push into regularArray, no poll wait needed
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            waitUntil(() -> historyWriter.events().stream().anyMatch(e -> e.type() == HistoryEventType.COMPLETED));
        } finally {
            engine.stop();
        }

        assertEquals(1, metricsReporter.completions().size());
        assertTrue(incidentStore.openIncidents().isEmpty());
        assertTrue(historyWriter.events().stream().anyMatch(e -> e.type() == HistoryEventType.STARTED));
    }

    @Test
    void exhaustsRetriesAndOpensIncident() throws Exception {
        NodeInfoProvider nodeInfoProvider = () -> "node-1";
        InMemoryTaskStore store = new InMemoryTaskStore();
        InMemoryClaimStrategy claimStrategy = new InMemoryClaimStrategy(
                store, nodeInfoProvider, new FixedDelayRetryPolicy(2, Duration.ZERO));
        InMemoryHeartbeatStrategy heartbeatStrategy = new InMemoryHeartbeatStrategy(claimStrategy);
        InMemoryTaskLedger taskLedger = new InMemoryTaskLedger(store);
        InMemoryHistoryWriter historyWriter = new InMemoryHistoryWriter();
        InMemoryAuditEventSink auditEventSink = new InMemoryAuditEventSink();
        InMemoryIncidentStore incidentStore = new InMemoryIncidentStore();
        InMemoryMetricsReporter metricsReporter = new InMemoryMetricsReporter();
        InMemoryPauseController pauseController = new InMemoryPauseController();
        InMemoryPayloadStore payloadStore = new InMemoryPayloadStore();

        AtomicInteger attempts = new AtomicInteger();

        TaskProcessor processor = new TaskProcessor() {
            @Override
            public String taskType() {
                return "FAIL";
            }

            @Override
            public void process(TaskContext ctx) {
                attempts.incrementAndGet();
                throw new RuntimeException("boom");
            }
        };

        TaskMeshEngine engine = TaskMeshEngine.builder()
                .nodeInfoProvider(nodeInfoProvider)
                .processors(List.of(processor))
                .claimStrategy(claimStrategy)
                .heartbeatStrategy(heartbeatStrategy)
                .taskLedger(taskLedger)
                .backlogPromoter(new InMemoryBacklogPromoter())
                .pauseController(pauseController)
                .historyWriter(historyWriter)
                .auditEventSink(auditEventSink)
                .incidentStore(incidentStore)
                .metricsReporter(metricsReporter)
                .payloadProviderFactory(new SingleProviderPayloadProviderFactory(payloadStore))
                .schedulingConfig(SchedulingConfig.builder().queryIntervalSeconds(1).build())
                .build();

        TaskRef taskRef = new TaskRef("FAIL", "t1");
        payloadStore.store(taskRef, new GreetingPayload("x"));

        engine.start();
        try {
            // First attempt via enqueue()'s direct push; the 2nd/3rd (retry) attempts
            // are only rediscovered by the engine's own poller (1s interval here).
            // maxAttempts=2 means the 3rd attempt is escalated to EXHAUSTED.
            engine.enqueue(taskRef);
            waitUntil(() -> !incidentStore.openIncidents().isEmpty());
        } finally {
            engine.stop();
        }

        assertTrue(attempts.get() >= 3, "expected 3 attempts before exhaustion, got " + attempts.get());
        assertFalse(incidentStore.openIncidents().isEmpty());
        assertFalse(metricsReporter.failures().isEmpty());
        assertTrue(metricsReporter.retries().size() >= 2);
    }

    @Test
    void adjustPoolSizeResizesWithinBuiltCeilingAndIgnoresOutOfRangeRequests() throws Exception {
        NodeInfoProvider nodeInfoProvider = () -> "node-1";
        InMemoryTaskStore store = new InMemoryTaskStore();
        InMemoryClaimStrategy claimStrategy = new InMemoryClaimStrategy(
                store, nodeInfoProvider, new FixedDelayRetryPolicy(3, Duration.ofMillis(10)));
        InMemoryHeartbeatStrategy heartbeatStrategy = new InMemoryHeartbeatStrategy(claimStrategy);
        InMemoryTaskLedger taskLedger = new InMemoryTaskLedger(store);

        TaskProcessor processor = new TaskProcessor() {
            @Override
            public String taskType() {
                return "NOOP";
            }

            @Override
            public void process(TaskContext ctx) {
            }
        };

        TaskMeshEngine engine = TaskMeshEngine.builder()
                .nodeInfoProvider(nodeInfoProvider)
                .processors(List.of(processor))
                .claimStrategy(claimStrategy)
                .heartbeatStrategy(heartbeatStrategy)
                .taskLedger(taskLedger)
                .backlogPromoter(new InMemoryBacklogPromoter())
                .pauseController(new InMemoryPauseController())
                .historyWriter(new InMemoryHistoryWriter())
                .auditEventSink(new InMemoryAuditEventSink())
                .incidentStore(new InMemoryIncidentStore())
                .metricsReporter(new InMemoryMetricsReporter())
                .payloadProviderFactory(new SingleProviderPayloadProviderFactory(new InMemoryPayloadStore()))
                .schedulingConfig(SchedulingConfig.builder().workerPoolSize(8).queryIntervalSeconds(1).build())
                .build();

        assertEquals(8, engine.currentPoolSize());

        engine.adjustPoolSize(3);
        assertEquals(3, engine.currentPoolSize());

        engine.adjustPoolSize(6);
        assertEquals(6, engine.currentPoolSize());

        // above the built ceiling (8) clamps down to it, not rejected/ignored
        engine.adjustPoolSize(100);
        assertEquals(8, engine.currentPoolSize());

        // below 1 clamps up to 1, never zero
        engine.adjustPoolSize(0);
        assertEquals(1, engine.currentPoolSize());
    }

    /**
     * Promotion is upstream of claiming, so a paused cluster must not keep creating
     * hot rows either. Without the check in {@code promoteBacklogOnce()}, a throttling
     * promoter would go on filling its in-flight window from a backlog that nobody is
     * draining — the one thing the window exists to prevent — and the poller's own
     * pause check would not stop it, because they run on separate schedulers.
     */
    @Test
    void aPausedClusterPromotesNoBacklog() throws Exception {
        CountingBacklogPromoter promoter = new CountingBacklogPromoter();
        InMemoryPauseController pauseController = new InMemoryPauseController();
        pauseController.requestPause();

        TaskMeshEngine engine = engineWithPromoter(promoter, pauseController);
        engine.start();
        try {
            // Several promotion cycles' worth of wall clock at a 1s backlog interval.
            Thread.sleep(2_500);
            assertEquals(0, promoter.calls.get(),
                    "a paused cluster must not promote — promotion is upstream of the claim the pause is meant to stop");

            pauseController.resume();
            waitUntil(() -> promoter.calls.get() > 0);
        } finally {
            engine.stop();
        }
        assertTrue(promoter.calls.get() > 0, "and promotion must resume once the pause is lifted");
    }

    /** Minimal engine whose only interesting wiring is the promoter and the pause controller. */
    private static TaskMeshEngine engineWithPromoter(CountingBacklogPromoter promoter,
                                                      InMemoryPauseController pauseController) {
        NodeInfoProvider nodeInfoProvider = () -> "node-1";
        InMemoryTaskStore store = new InMemoryTaskStore();
        InMemoryClaimStrategy claimStrategy = new InMemoryClaimStrategy(
                store, nodeInfoProvider, new FixedDelayRetryPolicy(3, Duration.ofMillis(10)));
        TaskProcessor processor = new TaskProcessor() {
            @Override
            public String taskType() {
                return "PROMOTE";
            }

            @Override
            public void process(TaskContext ctx) {
            }
        };
        return TaskMeshEngine.builder()
                .nodeInfoProvider(nodeInfoProvider)
                .processors(List.of(processor))
                .claimStrategy(claimStrategy)
                .heartbeatStrategy(new InMemoryHeartbeatStrategy(claimStrategy))
                .taskLedger(new InMemoryTaskLedger(store))
                .backlogPromoter(promoter)
                .pauseController(pauseController)
                .historyWriter(new InMemoryHistoryWriter())
                .auditEventSink(new InMemoryAuditEventSink())
                .incidentStore(new InMemoryIncidentStore())
                .metricsReporter(new InMemoryMetricsReporter())
                .payloadProviderFactory(new SingleProviderPayloadProviderFactory(new InMemoryPayloadStore()))
                .schedulingConfig(SchedulingConfig.builder()
                        .queryIntervalSeconds(1)
                        .backlogIntervalSeconds(1)
                        .build())
                .build();
    }

    static final class CountingBacklogPromoter implements BacklogPromoter {
        final AtomicInteger calls = new AtomicInteger();
        /** Throw on this call number (1-based); 0 = never throw. */
        volatile int throwOnCall;

        @Override
        public void onEnqueue(TaskRef taskRef, String nodeId) {
        }

        @Override
        public int promote(String taskType, java.time.Instant now, int limit) {
            int call = calls.incrementAndGet();
            if (call == throwOnCall) {
                throw new IllegalStateException("simulated promotion failure on call " + call);
            }
            return 0;
        }
    }

    /**
     * A throwing promoter must not retire the promotion schedule.
     *
     * <p>{@code scheduleWithFixedDelay} suppresses all subsequent executions once its task throws,
     * and the exception is captured into a {@code ScheduledFuture} nobody inspects — no handler
     * fires, nothing is logged, and the scheduler thread stays alive and idle. Observed for real
     * against Postgres: two of three nodes' promoters took one duplicate-key collision and never
     * promoted again for the life of the JVM. The only symptom was a slow queue.
     */
    @Test
    void aThrowingPromoterDoesNotRetireThePromotionSchedule() throws Exception {
        CountingBacklogPromoter promoter = new CountingBacklogPromoter();
        promoter.throwOnCall = 1;   // fail the very first cycle

        TaskMeshEngine engine = engineWithPromoter(promoter, new InMemoryPauseController());
        engine.start();
        try {
            waitUntil(() -> promoter.calls.get() >= 4);
        } finally {
            engine.stop();
        }
        assertTrue(promoter.calls.get() >= 4,
                "promotion must keep running after a failed cycle, not be silently suppressed — got only "
                        + promoter.calls.get() + " call(s)");
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 200 && !condition.getAsBoolean(); i++) {
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "condition not met within timeout");
    }
}
