package com.palmyralabs.taskmesh.it;

import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.simple.entity.TaskId;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmHistoryEventEntity;
import com.palmyralabs.taskmesh.simple.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.simple.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.simple.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.simple.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end proof of D33's counter model through the <b>real engine</b> —
 * poller, worker pool, {@code TaskContext}, {@code HistoryWriter},
 * {@code IncidentStore} and the operator {@code rerun} path — rather than
 * against a ledger called directly. What the processor sees, what gets written
 * to history, and what survives on the cold row all have to agree.
 *
 * <p>Own H2 database, per the isolation note on {@link EngineEndToEndTest}.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:counterdb;DB_CLOSE_DELAY=-1"
})
class CounterModelEngineTest {

    @Autowired private TaskMeshEngine engine;
    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmHistoryEventRepository historyEventRepository;
    @Autowired private TmInFlightRepository inFlightRepository;
    @Autowired private TmIncidentRepository incidentRepository;
    @Autowired private TaskmeshTestApp.CounterRecordingProcessor processor;

    /**
     * The scenario D33 exists for, driven end-to-end: a task fails its way
     * through the entire retry budget, is re-run by an operator, and actually
     * gets somewhere the second time instead of re-exhausting immediately.
     */
    @Test
    void aTaskThatExhaustsItsBudgetCanBeRerunAndGetsAFreshOneEndToEnd() {
        TaskRef ref = new TaskRef("COUNTERS", "e2e-1");
        engine.enqueue(ref);

        // retryPolicy is maxRetries=4, so attempt 1 runs 5 times: retries 0..4.
        awaitStatus(ref, TaskStatus.FAILED, Duration.ofSeconds(30));
        assertTrue(incidentRepository.findFirstByTaskTypeAndTaskIdAndResolvedAtIsNull("COUNTERS", "e2e-1").isPresent(),
                "exhaustion must open an incident");

        List<String> attemptOne = processor.observedPairs("e2e-1");
        assertEquals("1:0", attemptOne.get(0), "the very first execution is (attempt 1, retry 0)");
        assertTrue(attemptOne.stream().allMatch(p -> p.startsWith("1:")),
                "automatic retry never moves the outer counter, saw " + attemptOne);
        assertEquals(List.of("1:0", "1:1", "1:2", "1:3", "1:4"), attemptOne,
                "the inner counter advances by exactly one per execution, and stops at the budget");

        TmTaskEntity afterExhaustion = taskRow(ref);
        assertEquals(1, afterExhaustion.getAttemptCount());
        assertEquals(4, afterExhaustion.getRetryCount(), "4 retries burned before it gave up");

        // ---- operator re-runs it, and this time the work succeeds ----
        processor.stopFailing("e2e-1");
        engine.rerun(ref, "an-operator");

        awaitStatus(ref, TaskStatus.COMPLETED, Duration.ofSeconds(30));

        List<String> all = processor.observedPairs("e2e-1");
        assertEquals("2:0", all.get(all.size() - 1),
                "the re-run execution is attempt 2 with a fresh inner counter, saw " + all);
        assertEquals(1, all.stream().filter(p -> p.equals("2:0")).count(),
                "and it ran exactly once — a fresh budget means it never had to retry");

        TmTaskEntity afterRerun = taskRow(ref);
        assertEquals(2, afterRerun.getAttemptCount(), "the cold snapshot records the second attempt");
        assertEquals(0, afterRerun.getRetryCount(), "which burned no retries at all");
    }

    /**
     * The engine stamps every {@code HistoryEvent} with the same pair the
     * processor saw, so the audit trail can distinguish "retried five times" from
     * "an operator re-ran it" — the question the old single counter couldn't answer.
     */
    @Test
    void historyEventsCarryThePairTheProcessorActuallySaw() {
        TaskRef ref = new TaskRef("COUNTERS", "e2e-2");
        engine.enqueue(ref);
        awaitStatus(ref, TaskStatus.FAILED, Duration.ofSeconds(30));

        List<TmHistoryEventEntity> events = historyEventRepository.findAll().stream()
                .filter(e -> e.getTaskId().equals("e2e-2"))
                .toList();
        assertFalse(events.isEmpty(), "the run must be on the record at all");
        assertTrue(events.stream().allMatch(e -> e.getAttemptCount() == 1),
                "no operator rerun happened, so every event belongs to attempt 1");

        // Every execution that ran must have a STARTED event, each with its own retry ordinal.
        List<Integer> started = events.stream()
                .filter(e -> e.getEventType().name().equals("STARTED"))
                .map(TmHistoryEventEntity::getRetryCount)
                .sorted()
                .toList();
        assertEquals(List.of(0, 1, 2, 3, 4), started,
                "one STARTED per execution, keyed by a distinct retry ordinal, saw " + started);
    }


    /**
     * The failure the old model could not see at all: a node that dies mid-execution
     * never releases, so nothing ever billed it and nothing ever terminated the task.
     *
     * <p>Seeds the cold/hot pair directly rather than going through {@code enqueue} —
     * an over-budget, unreserved hot row is exactly what a run of dead executions
     * leaves behind, and building it outright keeps the test free of any race with
     * the engine's own poller. The hot row is left unreserved ({@code status = null}),
     * which is the "claimable by any node" shape.
     */
    @Test
    void aTaskWhosePreviousExecutionDiedIsTerminatedAtClaimAndReported() {
        TaskRef ref = new TaskRef("COUNTERS", "e2e-3");
        Instant now = Instant.now();

        TmTaskEntity coldRow = new TmTaskEntity();
        coldRow.setTaskType(ref.taskType());
        coldRow.setTaskId(ref.taskId());
        coldRow.setStatus(TaskStatus.QUEUED);
        coldRow.setAttemptCount(0);
        coldRow.setRetryCount(0);
        coldRow.setMaxRetries(Integer.MAX_VALUE);
        coldRow.setCreatedAt(now);
        coldRow.setUpdatedAt(now);
        taskRepository.save(coldRow);

        TmInFlightEntity hotRow = new TmInFlightEntity();
        hotRow.setTaskType(ref.taskType());
        hotRow.setTaskId(ref.taskId());
        hotRow.setStatus(null);
        hotRow.setAttemptCount(1);
        // retryPolicy is maxRetries=4, so ordinals 0..4 are admitted; a row already sitting on 4
        // has one execution left, and the claim that would start ordinal 5 must terminate it.
        hotRow.setRetryCount(4);
        hotRow.setMaxRetries(Integer.MAX_VALUE);
        hotRow.setCreatedAt(now);
            hotRow.setNextRetryAt(now);   // an externally-populated row must declare when it becomes visible
        hotRow.setUpdatedAt(now);
        inFlightRepository.save(hotRow);

        awaitStatus(ref, TaskStatus.FAILED, Duration.ofSeconds(30));

        assertTrue(inFlightRepository.findById(new TaskId("COUNTERS", "e2e-3")).isEmpty(),
                "claim-time exhaustion is terminal — the hot row must be deleted");
        assertEquals("RETRY_BUDGET_EXHAUSTED", taskRow(ref).getReasonCode());
        assertEquals(4, taskRow(ref).getRetryCount(),
                "the snapshot records the last ordinal that ran, not the one that never started");
        assertTrue(incidentRepository.findFirstByTaskTypeAndTaskIdAndResolvedAtIsNull("COUNTERS", "e2e-3").isPresent(),
                "silence is a bug: the operator has to be told, exactly as on the release path");
        assertTrue(processor.observedPairs("e2e-3").isEmpty(),
                "and the over-budget execution never actually ran");
    }

    // ---- helpers -------------------------------------------------------

    private TmTaskEntity taskRow(TaskRef ref) {
        return taskRepository.findById(new TaskId(ref.taskType(), ref.taskId())).orElseThrow();
    }

    private void awaitStatus(TaskRef ref, TaskStatus expected, Duration timeout) {
        waitUntil(() -> taskRepository.findById(new TaskId(ref.taskType(), ref.taskId()))
                .map(row -> row.getStatus() == expected)
                .orElse(false), timeout, "status " + expected + " for " + ref);
    }

    private void waitUntil(BooleanSupplier condition, Duration timeout, String what) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        fail("timed out waiting for " + what);
    }
}
