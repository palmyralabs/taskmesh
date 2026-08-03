package com.palmyralabs.taskmesh.multinode;

import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.simple.repository.TmAuditEventRepository;
import com.palmyralabs.taskmesh.simple.repository.TmControlFlagRepository;
import com.palmyralabs.taskmesh.simple.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.simple.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.simple.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;
import com.palmyralabs.taskmesh.simple.starter.TaskmeshProfileBuilder;
import com.palmyralabs.taskmesh.starter.TaskmeshProperties;
import com.palmyralabs.taskmesh.starter.WorkloadProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Soak/scale test: 50,000 tasks, each sleeping a random 2–5s, across 4 engines
 * sharing one H2 database. Validates no-loss delivery, cross-node balancing, and
 * that the lock-order fix holds under sustained enqueue+claim+release churn.
 */
@SpringBootTest(classes = MultiNodeTestApp.class, properties = {
        "spring.datasource.url=jdbc:h2:mem:largedb;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.datasource.hikari.maximum-pool-size=120"
})
class LargeScaleMultiNodeTest {

    private static final int NODES = 4;
    private static final int TASKS = 50_000;
    private static final int WORKERS_PER_NODE = 64;

    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmInFlightRepository inFlightRepository;
    @Autowired private TmHistoryEventRepository historyEventRepository;
    @Autowired private TmAttemptHistoryRepository attemptHistoryRepository;
    @Autowired private TmAuditEventRepository auditEventRepository;
    @Autowired private TmIncidentRepository incidentRepository;
    @Autowired private TmControlFlagRepository controlFlagRepository;
    @Autowired @Qualifier("taskmeshTransactionManager") private PlatformTransactionManager transactionManager;
    @Autowired private MultiNodeTestApp.RandomDelayProcessor processor;
    @Autowired private PayloadProviderFactory payloadProviderFactory;

    private final List<TaskMeshEngine> engines = new ArrayList<>();

    @BeforeEach
    void setUp() {
        for (int i = 1; i <= NODES; i++) {
            engines.add(engineFor("node-" + i));
        }
    }

    @AfterEach
    void tearDown() {
        engines.forEach(TaskMeshEngine::stop);
    }

    private TaskMeshEngine engineFor(String nodeId) {
        TaskmeshProperties props = new TaskmeshProperties();
        props.setReservationSeconds(2);
        props.setWorkerPoolSize(WORKERS_PER_NODE);
        props.setQueryIntervalSeconds(1);
        props.setLeaseSeconds(30);        // >> max task time (5s), so no false steal while a task runs
        props.setClaimTimeoutSeconds(30);
        return TaskmeshProfileBuilder.profile(WorkloadProfile.HIGH_VOLUME)
                .properties(props)
                .taskRepository(taskRepository)
                .inFlightRepository(inFlightRepository)
                .historyEventRepository(historyEventRepository)
                .attemptHistoryRepository(attemptHistoryRepository)
                .auditEventRepository(auditEventRepository)
                .incidentRepository(incidentRepository)
                .controlFlagRepository(controlFlagRepository)
                .transactionManager(transactionManager)
                .processors(List.of(processor))
                .payloadProviderFactory(payloadProviderFactory)
                .nodeInfoProvider(() -> nodeId)
                .build();
    }

    @Test
    void fiftyThousandTasksAcrossFourNodesCompleteWithNoLoss() throws Exception {
        Instant start = Instant.now();

        // Enqueue round-robin across the 4 nodes (parallel, to feed the cluster quickly).
        ExecutorService enq = Executors.newFixedThreadPool(8);
        for (int i = 0; i < TASKS; i++) {
            final int idx = i;
            enq.submit(() -> engines.get(idx % NODES).enqueue(new TaskRef("RND", "big-" + idx)));
        }
        enq.shutdown();
        assertTrue(enq.awaitTermination(5, TimeUnit.MINUTES), "all enqueues submitted");
        System.out.println("BIG enqueued " + TASKS + " in " + Duration.between(start, Instant.now()));

        await(() -> completedRnd() >= TASKS, Duration.ofMinutes(15));

        long completed = completedRnd();
        Duration elapsed = Duration.between(start, Instant.now());
        System.out.println("BIG done: completed=" + completed + "/" + TASKS + " in " + elapsed
                + " | runs node-1=" + processor.runsBy("node-1") + " node-2=" + processor.runsBy("node-2")
                + " node-3=" + processor.runsBy("node-3") + " node-4=" + processor.runsBy("node-4")
                + " totalRuns=" + processor.totalRuns() + " duplicateRuns=" + (processor.totalRuns() - TASKS));

        assertEquals(TASKS, completed, "every task must complete exactly once in the ledger (no loss)");
        for (int i = 1; i <= NODES; i++) {
            assertTrue(processor.runsBy("node-" + i) > 0, "node-" + i + " must have done work");
        }
    }

    /** Cheap aggregate count of COMPLETED RND rows (avoids loading 50k entities each poll). */
    private long completedRnd() {
        return taskRepository.countByTaskType("RND").stream()
                .filter(r -> r[0] == TaskStatus.COMPLETED)
                .mapToLong(r -> ((Number) r[1]).longValue())
                .findFirst().orElse(0L);
    }

    private void await(BooleanSupplier condition, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("only " + completedRnd() + "/" + TASKS + " completed within " + timeout);
    }
}
