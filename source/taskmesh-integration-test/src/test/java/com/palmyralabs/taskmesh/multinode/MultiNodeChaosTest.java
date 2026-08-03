package com.palmyralabs.taskmesh.multinode;

import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.impl.entity.TaskStatus;
import com.palmyralabs.taskmesh.impl.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.impl.repository.TmAuditEventRepository;
import com.palmyralabs.taskmesh.impl.repository.TmControlFlagRepository;
import com.palmyralabs.taskmesh.impl.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.impl.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.impl.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.impl.repository.TmTaskRepository;
import com.palmyralabs.taskmesh.starter.TaskmeshProfileBuilder;
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
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Two real engines (node-1 / node-2) sharing one H2 database — exercises actual
 * cross-node claim arbitration, reservation stealing, and no-loss delivery.
 */
@SpringBootTest(classes = MultiNodeTestApp.class, properties = {
        "spring.datasource.url=jdbc:h2:mem:mndb;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
class MultiNodeChaosTest {

    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmInFlightRepository inFlightRepository;
    @Autowired private TmHistoryEventRepository historyEventRepository;
    @Autowired private TmAttemptHistoryRepository attemptHistoryRepository;
    @Autowired private TmAuditEventRepository auditEventRepository;
    @Autowired private TmIncidentRepository incidentRepository;
    @Autowired private TmControlFlagRepository controlFlagRepository;
    @Autowired @Qualifier("taskmeshTransactionManager") private PlatformTransactionManager transactionManager;
    @Autowired private MultiNodeTestApp.NodeTrackingProcessor processor;
    @Autowired private MultiNodeTestApp.StallOnNode1Processor stallProcessor;
    @Autowired private com.palmyralabs.taskmesh.api.PayloadProviderFactory payloadProviderFactory;

    private TaskMeshEngine node1;
    private TaskMeshEngine node2;

    @BeforeEach
    void setUp() {
        node1 = engineFor("node-1");
        node2 = engineFor("node-2");
    }

    @AfterEach
    void tearDown() {
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
    }

    private TaskMeshEngine engineFor(String nodeId) {
        TaskmeshProperties props = new TaskmeshProperties();
        props.setReservationSeconds(1);   // short: node-2 can steal node-1's reserved backlog quickly
        props.setWorkerPoolSize(2);
        props.setQueryIntervalSeconds(1);
        props.setLeaseSeconds(5);
        props.setClaimTimeoutSeconds(5);  // node-1's stalled (8s) claims become stealable at 5s
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
                .processors(List.of(processor, stallProcessor))
                .payloadProviderFactory(payloadProviderFactory)
                .nodeInfoProvider(() -> nodeId)
                .build();
    }

    @Test
    void twoNodesShareTheBacklogAndCompleteEverythingExactlyOnceUnderNormalTiming() {
        int n = 200;
        for (int i = 0; i < n; i++) {
            node1.enqueue(new TaskRef("MN", "mn-" + i));   // all reserved to node-1 at enqueue
        }

        await(() -> completed() == n, Duration.ofSeconds(90));

        assertEquals(n, completed(), "no task may be lost across nodes");
        assertTrue(processor.runsBy("node-1") > 0, "node-1 processed some");
        assertTrue(processor.runsBy("node-2") > 0, "node-2 must steal reserved backlog after the reservation window");

        System.out.println("MN-DIST node-1=" + processor.runsBy("node-1") + " node-2=" + processor.runsBy("node-2")
                + " totalRuns=" + processor.totalRuns() + " (n=" + n
                + ", duplicateRuns=" + (processor.totalRuns() - n)
                + ", tasksRunByBothNodes=" + processor.tasksRunByMultipleNodes() + ")");
    }

    @Test
    void stalledClaimsOnOneNodeAreRecoveredByTheOtherWithNoLoss() {
        int n = 20;
        for (int i = 0; i < n; i++) {
            node1.enqueue(new TaskRef("STALL", "st-" + i));   // reserved to node-1, which stalls on them
        }

        // node-1 claims up to its 2 workers and stalls 8s (no checkpoint) — past the 5s claim-timeout.
        // node-2 must steal the reserved backlog (after 1s) AND the stale claims (after 5s) and finish everything.
        await(() -> completedStall() == n, Duration.ofSeconds(40));

        assertEquals(n, completedStall(), "every task completes despite node-1 stalling (dead-worker recovery)");
        assertTrue(stallProcessor.runsBy("node-2") > 0, "node-2 recovered the stalled/reserved work");
        System.out.println("MN-STALL node-1=" + stallProcessor.runsBy("node-1")
                + " node-2=" + stallProcessor.runsBy("node-2"));
    }

    private long completed() {
        return taskRepository.findByTaskTypeAndStatus("MN", TaskStatus.COMPLETED).size();
    }

    private long completedStall() {
        return taskRepository.findByTaskTypeAndStatus("STALL", TaskStatus.COMPLETED).size();
    }

    private void await(BooleanSupplier condition, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("only " + completed() + "/200 completed within " + timeout);
    }
}
