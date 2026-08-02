package com.palmyralabs.taskmesh.engine;

import com.palmyralabs.taskmesh.api.HistoryEventType;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.api.TypedTaskProcessor;
import com.palmyralabs.taskmesh.engine.impl.SchedulingConfig;
import com.palmyralabs.taskmesh.testkit.FixedDelayRetryPolicy;
import com.palmyralabs.taskmesh.testkit.InMemoryAuditEventSink;
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

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 200 && !condition.getAsBoolean(); i++) {
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "condition not met within timeout");
    }
}
