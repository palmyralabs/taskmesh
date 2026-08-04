package com.palmyralabs.taskmesh.it;

import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.simple.entity.TaskId;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.simple.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Adversarial scenarios — deliberately trying to break the engine end-to-end
 * against H2: retry/backoff, exhaustion, pause gating, concurrent duplicate
 * enqueue, and high-volume churn.
 *
 * <p>Isolated on its own H2 database — see {@link EngineEndToEndTest}'s Javadoc for why a bare
 * {@code @SpringBootTest} here would otherwise share Spring's cached context (and hence the live
 * {@code engine} bean) with that class, causing intermittent cross-test interference.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:scenariodb;DB_CLOSE_DELAY=-1"
})
class ScenarioExplorationTest {

    @Autowired private TaskMeshEngine engine;
    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmIncidentRepository incidentRepository;
    @Autowired private TaskmeshTestApp.FlakyProcessor flaky;
    @Autowired private TaskmeshTestApp.CountingProcessor counting;

    @Test
    void flakyTaskRetriesTwiceThenCompletes() {
        TaskRef ref = new TaskRef("FLAKY", "flaky-1");
        engine.enqueue(ref);

        awaitStatus(ref, TaskStatus.COMPLETED, Duration.ofSeconds(30));
        // at-least-once: the retry path drives it to success (>= 2 failures then a success). Exact
        // counts aren't guaranteed — under contention a task can be executed/requeued more than the
        // minimum, so these are lower bounds, not equalities.
        assertTrue(flaky.attempts("flaky-1") >= 3, "at least 2 failures then a success");
        assertEquals(1, taskRow(ref).getAttemptCount(), "no operator rerun happened, so it stays attempt 1 (D33)");
        assertTrue(taskRow(ref).getRetryCount() >= 2, "at least the 2 retries recorded on the inner counter");
    }

    @Test
    void alwaysRetryableFailureExhaustsToFailedAndOpensIncident() {
        TaskRef ref = new TaskRef("RETRYFAIL", "exhaust-1");
        engine.enqueue(ref);

        awaitStatus(ref, TaskStatus.FAILED, Duration.ofSeconds(30));
        assertTrue(incidentRepository.findFirstByTaskTypeAndTaskIdAndResolvedAtIsNull("RETRYFAIL", "exhaust-1").isPresent(),
                "retry-exhaustion must open an incident");
    }

    @Test
    void pausedEngineDoesNotProcessNewlyEnqueuedWork() {
        engine.pause("scenario-test");
        try {
            TaskRef ref = new TaskRef("OK", "paused-1");
            engine.enqueue(ref);

            // Documented contract (docs/schema.md): while paused the poller stops
            // claiming new work — so this must NOT complete until we resume.
            sleep(Duration.ofSeconds(4));
            TmTaskEntity row = taskRepository.findById(new TaskId("OK", "paused-1")).orElseThrow();
            assertEquals(TaskStatus.QUEUED, row.getStatus(),
                    "a task enqueued while paused must not be processed until resume");
        } finally {
            engine.resume("scenario-test");
        }

        awaitStatus(new TaskRef("OK", "paused-1"), TaskStatus.COMPLETED, Duration.ofSeconds(15));
    }

    @Test
    void concurrentDuplicateEnqueueStaysConsistent() throws Exception {
        int threads = 25;
        TaskRef ref = new TaskRef("OK", "dup-1");
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    engine.enqueue(ref);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS), "enqueue threads finished");

        awaitStatus(ref, TaskStatus.COMPLETED, Duration.ofSeconds(15));
        // taskmesh is at-least-once, not exactly-once (D4/D11) — a same-id enqueue racing a
        // completion may run more than once; idempotency is the processor's job. What the
        // engine MUST guarantee: a single durable ledger row that ends COMPLETED, uncorrupted.
        assertTrue(counting.count("dup-1") >= 1, "processed at least once");
        assertEquals(TaskStatus.COMPLETED, taskRow(ref).getStatus(), "exactly one ledger row, ending COMPLETED");
    }

    @Test
    void highVolumeAllComplete() {
        int n = 500;
        for (int i = 0; i < n; i++) {
            engine.enqueue(new TaskRef("OK", "vol-" + i));
        }
        await(() -> taskRepository.findByTaskTypeAndStatus("OK", TaskStatus.COMPLETED).stream()
                        .filter(t -> t.getTaskId().startsWith("vol-")).count() == n,
                Duration.ofSeconds(60));
    }

    // ---- helpers ----

    private TmTaskEntity taskRow(TaskRef ref) {
        return taskRepository.findById(new TaskId(ref.taskType(), ref.taskId())).orElseThrow();
    }

    private void awaitStatus(TaskRef ref, TaskStatus expected, Duration timeout) {
        await(() -> {
            TmTaskEntity row = taskRepository.findById(new TaskId(ref.taskType(), ref.taskId())).orElse(null);
            return row != null && row.getStatus() == expected;
        }, timeout);
    }

    private void await(BooleanSupplier condition, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(Duration.ofMillis(100));
        }
        fail("condition not met within " + timeout);
    }

    private void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
