package com.palmyralabs.taskmesh.it;

import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.impl.entity.TaskId;
import com.palmyralabs.taskmesh.impl.entity.TaskStatus;
import com.palmyralabs.taskmesh.impl.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.impl.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.impl.repository.TmTaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Drives the whole stack — Spring Boot starter auto-config + JPA ledger +
 * engine poller/worker pool — against H2, through real enqueue/rerun flows.
 */
@SpringBootTest
class EngineEndToEndTest {

    @Autowired
    private TaskMeshEngine engine;

    @Autowired
    private TaskmeshTestApp.CountingProcessor counting;

    @Autowired
    private TmTaskRepository taskRepository;

    @Autowired
    private TmIncidentRepository incidentRepository;

    @Test
    void enqueuedTaskIsClaimedProcessedAndCompleted() {
        TaskRef ref = new TaskRef("OK", "e2e-1");

        engine.enqueue(ref);

        awaitTaskStatus(ref, TaskStatus.COMPLETED);
        assertEquals(1, counting.count("e2e-1"), "the processor ran exactly once");
    }

    @Test
    void nonRetryableFailureLandsTerminalAndOpensAnIncident() {
        TaskRef ref = new TaskRef("FAIL", "e2e-2");

        engine.enqueue(ref);

        awaitTaskStatus(ref, TaskStatus.FAILED);
        await(() -> incidentRepository
                .findFirstByTaskTypeAndTaskIdAndResolvedAtIsNull("FAIL", "e2e-2").isPresent());
        assertTrue(incidentRepository.findFirstByTaskTypeAndTaskIdAndResolvedAtIsNull("FAIL", "e2e-2").isPresent(),
                "a terminal non-retryable failure must open an incident");
    }

    @Test
    void rerunReprocessesACompletedTask() {
        TaskRef ref = new TaskRef("OK", "e2e-3");
        engine.enqueue(ref);
        awaitTaskStatus(ref, TaskStatus.COMPLETED);
        assertEquals(1, counting.count("e2e-3"));

        engine.rerun(ref, "integration-test");

        await(() -> counting.count("e2e-3") == 2);
        awaitTaskStatus(ref, TaskStatus.COMPLETED);
        assertEquals(2, counting.count("e2e-3"), "rerun caused a second processing pass");
    }

    // ---- helpers ----

    private void awaitTaskStatus(TaskRef ref, TaskStatus expected) {
        await(() -> {
            TmTaskEntity row = taskRepository.findById(new TaskId(ref.taskType(), ref.taskId())).orElse(null);
            return row != null && row.getStatus() == expected;
        });
    }

    private void await(BooleanSupplier condition) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while awaiting condition");
            }
        }
        fail("condition not met within timeout");
    }
}
