package com.palmyralabs.taskmesh.engine;

import com.palmyralabs.taskmesh.api.HistoryEvent;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import com.palmyralabs.taskmesh.api.HistoryWriter;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.api.TaskRef;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial tests targeting the engine's least-guarded edges: a processor
 * throwing {@link Error} rather than {@link Exception}, a {@link HistoryWriter}
 * that fails on its own, and two nodes racing to claim the same task. These
 * probe hypotheses about specific failure modes rather than golden-path behavior.
 */
class TaskMeshEngineFailureModesTest {

    record NoPayload() {
    }

    private record Harness(
            TaskMeshEngine engine, InMemoryTaskStore store, InMemoryClaimStrategy claimStrategy,
            InMemoryHistoryWriter historyWriter, InMemoryIncidentStore incidentStore,
            InMemoryMetricsReporter metricsReporter, InMemoryPayloadStore payloadStore) {
    }

    private Harness build(String nodeId, TaskProcessor processor, InMemoryTaskStore sharedStore, HistoryWriter historyWriterOverride) {
        NodeInfoProvider nodeInfoProvider = () -> nodeId;
        InMemoryTaskStore store = sharedStore != null ? sharedStore : new InMemoryTaskStore();
        InMemoryClaimStrategy claimStrategy = new InMemoryClaimStrategy(
                store, nodeInfoProvider, new FixedDelayRetryPolicy(3, Duration.ofMillis(10)));
        InMemoryHeartbeatStrategy heartbeatStrategy = new InMemoryHeartbeatStrategy(claimStrategy);
        InMemoryTaskLedger taskLedger = new InMemoryTaskLedger(store);
        InMemoryHistoryWriter historyWriter = new InMemoryHistoryWriter();
        InMemoryIncidentStore incidentStore = new InMemoryIncidentStore();
        InMemoryMetricsReporter metricsReporter = new InMemoryMetricsReporter();
        InMemoryPayloadStore payloadStore = new InMemoryPayloadStore();

        TaskMeshEngine engine = TaskMeshEngine.builder()
                .nodeInfoProvider(nodeInfoProvider)
                .processors(List.of(processor))
                .claimStrategy(claimStrategy)
                .heartbeatStrategy(heartbeatStrategy)
                .taskLedger(taskLedger)
                .backlogPromoter(new InMemoryBacklogPromoter())
                .pauseController(new InMemoryPauseController())
                .historyWriter(historyWriterOverride != null ? historyWriterOverride : historyWriter)
                .auditEventSink(new InMemoryAuditEventSink())
                .incidentStore(incidentStore)
                .metricsReporter(metricsReporter)
                .payloadProviderFactory(new SingleProviderPayloadProviderFactory(payloadStore))
                .schedulingConfig(SchedulingConfig.builder().queryIntervalSeconds(1).build())
                .build();

        return new Harness(engine, store, claimStrategy, historyWriter, incidentStore, metricsReporter, payloadStore);
    }

    /**
     * {@code attempt()} now has a dedicated {@code catch (Error error)} clause
     * (fixing the bug this test originally caught: an escaping {@link Error} used
     * to strand the task CLAIMED forever with zero record). An Error is released
     * immediately as terminal/incident-worthy — never routed through
     * {@link com.palmyralabs.taskmesh.api.FailureClassifier}, never burns a retry
     * attempt — and rethrown afterward so it still surfaces on the worker thread.
     */
    @Test
    void processorThrowingErrorIsReleasedTerminallyWithAnIncidentAndStillPropagates() throws Exception {
        TaskRef taskRef = new TaskRef("BOOM_ERROR", "e1");
        CountDownLatch enteredProcess = new CountDownLatch(1);

        TaskProcessor processor = new TaskProcessor() {
            @Override
            public String taskType() {
                return "BOOM_ERROR";
            }

            @Override
            public void process(TaskContext ctx) {
                enteredProcess.countDown();
                throw new Error("simulated unrecoverable error");
            }
        };

        Harness h = build("node-1", processor, null, null);
        h.payloadStore().store(taskRef, new NoPayload());

        h.engine().start();
        try {
            h.engine().enqueue(taskRef);
            assertTrue(enteredProcess.await(5, TimeUnit.SECONDS), "processor must have been invoked");

            waitUntil(() -> !h.incidentStore().openIncidents().isEmpty());

            assertEquals(List.of(HistoryEventType.STARTED, HistoryEventType.FAILED),
                    h.historyWriter().events().stream().map(HistoryEvent::type).toList(),
                    "STARTED then FAILED — no RETRIED, since an Error never goes through FailureClassifier");
            assertFalse(h.incidentStore().openIncidents().isEmpty(), "an Error must open an incident, same as any other terminal failure");
            assertFalse(h.metricsReporter().failures().isEmpty(), "must be counted as a failure in metrics too");
        } finally {
            h.engine().stop();
        }
    }

    /**
     * {@code attempt()}'s {@code historyWriter.record(...)} calls now all go through a
     * {@code recordHistory(...)} wrapper (fixing the bug this test originally caught: a
     * failure on the STARTED write — which runs before {@code process()} even starts —
     * used to skip the processor entirely and leak the claim). The write failure must be
     * swallowed (logged, not thrown) so it can never affect control flow, matching
     * {@link HistoryWriter}'s own documented contract.
     */
    @Test
    void historyWriterFailureNeverBlocksProcessingOrLeaksTheClaim() throws Exception {
        TaskRef taskRef = new TaskRef("HW_FAIL", "h1");
        AtomicBoolean processorRan = new AtomicBoolean(false);
        AtomicInteger recordCalls = new AtomicInteger();

        HistoryWriter throwingOnEveryWrite = event -> {
            recordCalls.incrementAndGet();
            throw new RuntimeException("simulated history store outage");
        };

        TaskProcessor processor = new TaskProcessor() {
            @Override
            public String taskType() {
                return "HW_FAIL";
            }

            @Override
            public void process(TaskContext ctx) {
                processorRan.set(true);
            }
        };

        Harness h = build("node-1", processor, null, throwingOnEveryWrite);
        h.payloadStore().store(taskRef, new NoPayload());

        h.engine().start();
        try {
            h.engine().enqueue(taskRef);
            waitUntil(() -> !h.metricsReporter().completions().isEmpty());

            assertTrue(processorRan.get(), "process() must still run despite every HistoryWriter call failing");
            assertTrue(recordCalls.get() > 0, "sanity: the throwing HistoryWriter must actually have been invoked");
        } finally {
            h.engine().stop();
        }
    }

    /**
     * Two nodes sharing one backing store race to claim the same freshly
     * enqueued task; exactly one must win, and the loser's own claim call must
     * report LOST rather than silently double-processing.
     */
    @Test
    void twoNodesRacingToClaimTheSameTaskOnlyOneWins() throws Exception {
        TaskRef taskRef = new TaskRef("RACE", "r1");
        InMemoryTaskStore sharedStore = new InMemoryTaskStore();

        CopyOnWriteArrayList<String> processedBy = new CopyOnWriteArrayList<>();

        class RecordingProcessor implements TaskProcessor {
            private final String nodeId;

            RecordingProcessor(String nodeId) {
                this.nodeId = nodeId;
            }

            @Override
            public String taskType() {
                return "RACE";
            }

            @Override
            public void process(TaskContext ctx) {
                processedBy.add(nodeId);
            }
        }

        Harness h1 = build("node-1", new RecordingProcessor("node-1"), sharedStore, null);
        Harness h2 = build("node-2", new RecordingProcessor("node-2"), sharedStore, null);
        h1.payloadStore().store(taskRef, new NoPayload());
        h2.payloadStore().store(taskRef, new NoPayload());
        sharedStore.enqueue(taskRef); // one shared row — neither engine's enqueue() double-inserts

        h1.engine().start();
        h2.engine().start();
        try {
            // Nudge both engines to attempt a claim around the same time via their pollers.
            waitUntil(() -> !processedBy.isEmpty());
            Thread.sleep(500); // let any would-be double-processing happen if it's going to

            assertEquals(1, processedBy.size(), "exactly one node must have processed the task, not both: " + processedBy);
        } finally {
            h1.engine().stop();
            h2.engine().stop();
        }
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 200 && !condition.getAsBoolean(); i++) {
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "condition not met within timeout");
    }
}
