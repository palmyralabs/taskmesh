package com.palmyralabs.taskmesh.it;

import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.simple.entity.TaskId;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.simple.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * {@code TaskProcessor.onTerminal} end-to-end through the real engine: which
 * transitions fire it, which deliberately do not, and what a throwing callback
 * does to the terminal transition it is vetoing.
 *
 * <p>Own H2 database, per the isolation note on {@link EngineEndToEndTest}.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:terminaldb;DB_CLOSE_DELAY=-1"
})
class TerminalCallbackTest {

    @Autowired private TaskMeshEngine engine;
    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmInFlightRepository inFlightRepository;
    @Autowired @org.springframework.beans.factory.annotation.Qualifier("taskmeshTransactionTemplate")
    private org.springframework.transaction.support.TransactionTemplate transactionTemplate;
    @Autowired private TaskmeshTestApp.TerminalRecordingProcessor processor;

    // ---- the three transitions that fire it -----------------------------

    @Test
    void aSuccessfulTaskNotifiesExactlyOnceWithCOMPLETED() {
        TaskRef ref = new TaskRef("TERMINAL", "ok-1");
        processor.mode("ok-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.SUCCEED);

        engine.enqueue(ref);
        awaitStatus(ref, TaskStatus.COMPLETED, Duration.ofSeconds(30));
        settle();

        assertEquals(List.of("COMPLETED/1:0"), processor.terminalsFor("ok-1"),
                "exactly one notification, carrying the counters of the execution that finished");
    }

    @Test
    void aNonRetryableFailureNotifiesExactlyOnceWithFAILED_NONRETRYABLE() {
        TaskRef ref = new TaskRef("TERMINAL", "bad-1");
        processor.mode("bad-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.FAIL_NONRETRYABLE);

        engine.enqueue(ref);
        awaitStatus(ref, TaskStatus.FAILED, Duration.ofSeconds(30));
        settle();

        assertEquals(List.of("FAILED_NONRETRYABLE/1:0"), processor.terminalsFor("bad-1"),
                "terminal on its first execution, with budget still unspent");
        assertEquals(1, processor.executions("bad-1"), "and it really was never retried");
    }

    @Test
    void aRetryableFailureNotifiesOnlyOnceTheBudgetIsSpentAndReportsFAILED_EXHAUSTED() {
        TaskRef ref = new TaskRef("TERMINAL", "dead-1");
        processor.mode("dead-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.FAIL_RETRYABLE);

        engine.enqueue(ref);
        awaitStatus(ref, TaskStatus.FAILED, Duration.ofSeconds(30));
        settle();

        // maxRetries=4, so ordinals 0..4 run and only the last one is terminal. Every earlier
        // failure was a within-budget RETRY, which is not a terminal transition at all.
        assertTrue(processor.executions("dead-1") > 1, "it retried before giving up");
        assertEquals(List.of("FAILED_EXHAUSTED/1:4"), processor.terminalsFor("dead-1"),
                "one notification only, on the execution that ran out of budget");
    }

    // ---- the transitions that must stay silent ---------------------------

    /**
     * A node whose claim was stolen mid-execution writes nothing on release, and so must announce
     * nothing either — telling a consumer "this finished" on the strength of a release that was
     * fenced out is precisely the divergence the callback exists to prevent.
     *
     * <p>Driven through the engine on purpose. Calling the ledger directly could not prove
     * anything here: the ledger has no idea the callback exists, so its silence is guaranteed by
     * construction rather than by the behaviour under test.
     */
    @Test
    void aReleaseThatLostOwnershipNotifiesNothing() throws Exception {
        TaskRef ref = new TaskRef("TERMINAL", "stolen-1");
        processor.mode("stolen-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.BLOCK_UNTIL_RELEASED);

        engine.enqueue(ref);
        processor.awaitStarted("stolen-1", Duration.ofSeconds(30));

        // Another node takes the claim while this one is still working.
        steal(ref);
        processor.releaseExecution("stolen-1");

        // Give the (now fenced-out) release every chance to misfire.
        settle();
        settle();

        assertEquals(List.of(), processor.terminalsFor("stolen-1"),
                "a node that wrote nothing must not tell the consumer the task finished");
        assertEquals(TaskStatus.QUEUED, taskRepository.findById(new TaskId("TERMINAL", "stolen-1"))
                .orElseThrow().getStatus(), "and the cold row must be untouched too");
    }

    /**
     * A within-budget {@code RETRY} is not terminal. Covered by
     * {@link #aRetryableFailureNotifiesOnlyOnceTheBudgetIsSpentAndReportsFAILED_EXHAUSTED}: that
     * task fails several times and is notified exactly once.
     *
     * <p>{@code YIELD} has no test here because the engine never issues one — it is a
     * consumer/operator-driven disposition on the ledger, and the tier suites cover that a yield
     * leaves the task alive and non-terminal.
     */

    // ---- onStart, and telling a re-run from a retry ----------------------

    /**
     * The signal that replaces a dedicated "re-run" callback. A consumer holding a derived
     * projection has to move that row back off its terminal state when an operator re-runs a
     * task, and nothing else in the engine tells it the task is alive again. The D33 pair does:
     * {@code retryCount == 0} with {@code attemptCount > 1} is a re-run and nothing else can be.
     */
    @Test
    void onStartDistinguishesAFirstRunFromARetryFromAnOperatorRerun() {
        TaskRef ref = new TaskRef("TERMINAL", "start-1");
        processor.mode("start-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.FAIL_RETRYABLE);

        engine.enqueue(ref);
        awaitStatus(ref, TaskStatus.FAILED, Duration.ofSeconds(30));
        settle();

        // Attempt 1: one start per execution, the inner counter advancing each time.
        assertEquals(List.of("1:0", "1:1", "1:2", "1:3", "1:4"), processor.startsFor("start-1"),
                "one start per execution, and only the first of them is retry 0");

        // Now an operator re-runs it, and this time the work succeeds.
        processor.mode("start-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.SUCCEED);
        engine.rerun(ref, "an-operator");
        awaitStatus(ref, TaskStatus.COMPLETED, Duration.ofSeconds(30));
        settle();

        List<String> starts = processor.startsFor("start-1");
        assertEquals("2:0", starts.get(starts.size() - 1),
                "the re-run announces itself as a new attempt at retry 0, saw " + starts);

        // The property a consumer actually keys on: within one attempt exactly one start has
        // retryCount 0, so writing only on those is one row-touch per attempt — the same volume
        // onTerminal costs, which is what makes the projection affordable at all.
        assertEquals(2, starts.stream().filter(p -> p.endsWith(":0")).count(),
                "exactly two attempt-starts across the whole life of this task, saw " + starts);
        assertEquals(List.of("FAILED_EXHAUSTED/1:4", "COMPLETED/2:0"), processor.terminalsFor("start-1"),
                "and each attempt ended exactly once, so starts and terminals pair up");
    }

    @Test
    void aThrowingOnStartAbortsTheExecutionAndIsRetriedRatherThanSwallowed() {
        TaskRef ref = new TaskRef("TERMINAL", "start-boom-1");
        processor.mode("start-boom-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.SUCCEED);
        processor.failNextStart("start-boom-1");

        engine.enqueue(ref);
        awaitStatus(ref, TaskStatus.COMPLETED, Duration.ofSeconds(30));
        settle();

        assertEquals(1, processor.executions("start-boom-1"),
                "the refused start must abort before process() runs, not after it");
        assertEquals(List.of("1:0", "1:1"), processor.startsFor("start-boom-1"),
                "and the failure is retried like any other, so the next execution starts normally");
        assertEquals(List.of("COMPLETED/1:1"), processor.terminalsFor("start-boom-1"));
    }

    // ---- vetoing a terminal transition ------------------------------------

    @Test
    void aThrowingCallbackRollsBackTheTerminalTransitionAndTheTaskRunsAgain() {
        TaskRef ref = new TaskRef("TERMINAL", "veto-1");
        processor.mode("veto-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.SUCCEED);
        processor.failTerminalTimes("veto-1", 1);   // refuse the first terminal transition

        engine.enqueue(ref);

        // The refusal must not leave a half-finished state: the cold row cannot be COMPLETED while
        // the consumer's own write was rolled back. The task comes round again instead.
        awaitStatus(ref, TaskStatus.COMPLETED, Duration.ofSeconds(30));
        settle();

        assertTrue(processor.executions("veto-1") >= 2,
                "the vetoed transition sent the task back round, so process() ran again; saw "
                        + processor.executions("veto-1"));
        assertEquals(List.of("COMPLETED/1:1"), processor.terminalsFor("veto-1"),
                "and only the transition that was actually allowed to commit was recorded");
        assertTrue(inFlightRepository.findById(new TaskId("TERMINAL", "veto-1")).isEmpty(),
                "which then completed normally, hot row and all");
    }

    /**
     * The transition {@code release()} can never report: a node died mid-execution, so nothing
     * released and nothing could. D33 terminates such a task at the next claim instead, and that
     * is just as terminal — a consumer projection that never hears about it is exactly the silent
     * divergence this callback exists to prevent.
     *
     * <p>Seeded directly rather than through {@code enqueue}: an over-budget, unreserved hot row
     * is what a run of dead executions leaves behind, and building it outright keeps the test free
     * of any race with the poller.
     */
    @Test
    void aTaskTerminatedAtClaimTimeNotifiesToo() {
        TaskRef ref = new TaskRef("TERMINAL", "claimdead-1");
        processor.mode("claimdead-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.SUCCEED);
        Instant now = Instant.now();

        transactionTemplate.executeWithoutResult(status -> {
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
            hotRow.setStatus(null);              // unreserved: claimable by any node
            hotRow.setAttemptCount(1);
            hotRow.setRetryCount(4);             // retryPolicy allows ordinals 0..4
            hotRow.setMaxRetries(Integer.MAX_VALUE);
            hotRow.setCreatedAt(now);
            hotRow.setUpdatedAt(now);
            inFlightRepository.save(hotRow);
        });

        awaitStatus(ref, TaskStatus.FAILED, Duration.ofSeconds(30));
        settle();

        assertEquals(List.of("FAILED_EXHAUSTED/1:4"), processor.terminalsFor("claimdead-1"),
                "the claim that terminated it must notify, with the last ordinal that actually ran");
        assertEquals(0, processor.executions("claimdead-1"), "and no execution ever ran");
        assertNull(processor.terminalPayload("claimdead-1"),
                "with no payload, since there was no execution to load one");
    }

    // ---- payload ----------------------------------------------------------

    @Test
    void aProcessorThatNeverReadsItsPayloadIsHandedNullRatherThanTriggeringALoad() {
        TaskRef ref = new TaskRef("TERMINAL", "nopayload-1");
        processor.mode("nopayload-1", TaskmeshTestApp.TerminalRecordingProcessor.Mode.SUCCEED);

        engine.enqueue(ref);
        awaitStatus(ref, TaskStatus.COMPLETED, Duration.ofSeconds(30));
        settle();

        assertEquals(List.of("COMPLETED/1:0"), processor.terminalsFor("nopayload-1"));
        assertNull(processor.terminalPayload("nopayload-1"),
                "the terminal transaction must not fetch a payload nobody asked for");
    }

    // ---- helpers -------------------------------------------------------

    /** Steals the claim by hand — no second engine needed to reach the fenced-out release path. */
    private void steal(TaskRef ref) {
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = inFlightRepository.findById(new TaskId(ref.taskType(), ref.taskId())).orElseThrow();
            row.setNodeId("some-other-node");
            inFlightRepository.save(row);
        });
    }

    /** Terminal bookkeeping runs just after the status write; give it a beat before asserting on it. */
    private void settle() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void awaitStatus(TaskRef ref, TaskStatus expected, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        BooleanSupplier reached = () -> taskRepository.findById(new TaskId(ref.taskType(), ref.taskId()))
                .map(row -> row.getStatus() == expected)
                .orElse(false);
        while (Instant.now().isBefore(deadline)) {
            if (reached.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        TmTaskEntity row = taskRepository.findById(new TaskId(ref.taskType(), ref.taskId())).orElse(null);
        fail("timed out waiting for " + expected + " on " + ref + ", status was "
                + (row == null ? "<no row>" : row.getStatus()));
    }
}
