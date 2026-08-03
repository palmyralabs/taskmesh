package com.palmyralabs.taskmesh.it;

import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;
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
 * Load shape: 100 instances of a multi-step processor that each run for >= 15s
 * while actively heartbeating via {@code ctx.checkpoint()}. Runs in its own
 * context with a 32-thread worker pool, so the 100 tasks drain in ~4 waves
 * (roughly 60s of wall time), exercising real queueing/hand-off — not just a
 * one-shot fan-out.
 */
@SpringBootTest(properties = {
        "taskmesh.worker-pool-size=32",
        "taskmesh.query-interval-seconds=1",
        "spring.datasource.hikari.maximum-pool-size=40"
})
class LongRunningLoadTest {

    private static final int INSTANCES = 100;

    @Autowired
    private TaskMeshEngine engine;

    @Autowired
    private TaskmeshTestApp.SlowMultiStepProcessor slow;

    @Autowired
    private TmTaskRepository taskRepository;

    @Test
    void hundredLongRunningTasksAllCompleteWithHeartbeating() {
        Instant start = Instant.now();

        for (int i = 0; i < INSTANCES; i++) {
            engine.enqueue(new TaskRef("SLOW", "load-" + i));
        }

        // Each task needs >= 15s, so even fully parallel the batch cannot finish before then.
        await(() -> completedRows() == INSTANCES, Duration.ofSeconds(120));
        Duration elapsed = Duration.between(start, Instant.now());

        assertEquals(INSTANCES, slow.completedCount(), "every processor instance ran to completion");
        assertEquals(INSTANCES, completedRows(), "every task_ledger row reached COMPLETED");
        assertTrue(elapsed.compareTo(Duration.ofSeconds(15)) >= 0,
                "each instance runs >= 15s, so the batch cannot complete sooner (was " + elapsed + ")");
    }

    private long completedRows() {
        return taskRepository.findByTaskTypeAndStatus("SLOW", TaskStatus.COMPLETED).size();
    }

    private void await(BooleanSupplier condition, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while awaiting condition");
            }
        }
        fail("only " + completedRows() + "/" + INSTANCES + " completed within " + timeout);
    }
}
