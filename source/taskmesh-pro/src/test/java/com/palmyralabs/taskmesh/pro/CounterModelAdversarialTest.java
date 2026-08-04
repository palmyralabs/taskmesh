package com.palmyralabs.taskmesh.pro;

import com.palmyralabs.taskmesh.pro.entity.TaskId;
import com.palmyralabs.taskmesh.pro.entity.TaskStatus;
import com.palmyralabs.taskmesh.pro.entity.TmAttemptHistoryEntity;
import com.palmyralabs.taskmesh.pro.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.pro.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.pro.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.pro.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.pro.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.AttemptCounters;
import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.HistoryEvent;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial probes against D33's nested {@code attempt_count}/{@code retry_count}
 * model — deliberately hostile sequences (lease loss, double release, stolen
 * claims, budget edges, re-run boundaries, concurrent reclaim) aimed at making
 * the counters double-bill, lose a bill, rewind, or silently overwrite history.
 *
 * <p>Two tests here document <b>known gaps</b> rather than desired behaviour;
 * both say so explicitly in-line. They are asserted so the gap can't widen
 * unnoticed, and so closing it later shows up as a deliberate test change.
 */
@DataJpaTest
class CounterModelAdversarialTest {

    @Autowired
    private TmTaskRepository taskRepository;

    @Autowired
    private TmInFlightRepository inFlightRepository;

    @Autowired
    private TmAttemptHistoryRepository attemptHistoryRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private MultiNodeTaskLedger node1;
    private MultiNodeTaskLedger node2;

    @BeforeEach
    void setUp() {
        transactionTemplate = new TransactionTemplate(transactionManager);
        node1 = ledgerFor("node-1", 3);
        node2 = ledgerFor("node-2", 3);
    }

    // leaseDuration=5min, reclaimWindow=30s, affinityWindow=60s, claimTimeout=10min, reservationWindow=60s.
    private MultiNodeTaskLedger ledgerFor(String nodeId, int maxRetries) {
        return ledgerFor(nodeId, new ExponentialBackoffRetryPolicy(maxRetries, Duration.ofMillis(1), Duration.ofMillis(1)));
    }

    private MultiNodeTaskLedger ledgerFor(String nodeId, ExponentialBackoffRetryPolicy retryPolicy) {
        return new MultiNodeTaskLedger(taskRepository, inFlightRepository, transactionTemplate, () -> nodeId,
                retryPolicy, Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ofSeconds(60),
                Duration.ofMinutes(10), Duration.ofSeconds(60));
    }

    // ---- helpers -------------------------------------------------------

    private TmInFlightEntity hot(TaskRef taskRef) {
        return inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElseThrow();
    }

    private TmTaskEntity cold(TaskRef taskRef) {
        return taskRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElseThrow();
    }

    /** The owning node died mid-execution: its lease lapses, and it never gets to release. */
    private void killOwnerAndMakeStealable(TaskRef taskRef) {
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = hot(taskRef);
            row.setLeaseExpiresAt(Instant.now().minus(Duration.ofMinutes(1)));
            row.setUpdatedAt(Instant.now().minus(Duration.ofMinutes(30)));   // past claimTimeout
            row.setNextRetryAt(Instant.EPOCH);   // "visible since forever" - the gate is a timestamp now, never null
            inFlightRepository.save(row);
        });
    }

    private void makeStealable(TaskRef taskRef) {
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = hot(taskRef);
            row.setUpdatedAt(Instant.now().minus(Duration.ofMinutes(30)));
            row.setNextRetryAt(Instant.EPOCH);   // "visible since forever" - the gate is a timestamp now, never null
            inFlightRepository.save(row);
        });
    }

    private void assertCounters(TaskRef taskRef, int attempt, int retry) {
        assertEquals(new AttemptCounters(attempt, retry), node1.counters(taskRef));
        assertEquals(attempt, hot(taskRef).getAttemptCount());
        assertEquals(retry, hot(taskRef).getRetryCount());
    }

    // ---- lease loss / reclaim billing ----------------------------------

    @Test
    void aNodeThatLostItsLeaseCannotBillAnythingOnItsWayOut() {
        TaskRef ref = new TaskRef("ADV", "lost-owner");
        node1.enqueue(ref);
        assertEquals(ClaimOutcome.WON, node1.claim(ref));

        killOwnerAndMakeStealable(ref);
        assertEquals(ClaimOutcome.WON, node2.claim(ref), "node-2 steals the dead lease");
        assertCounters(ref, 1, 1);

        // node-1 finally comes back and tries to release. Every release path is fenced by node_id,
        // so it can neither bill a retry nor terminate a task it no longer owns.
        assertEquals(ReleaseOutcome.LOST_OWNERSHIP, node1.release(ref, ReleaseRequest.retry("BOOM", "late")));
        assertEquals(ReleaseOutcome.LOST_OWNERSHIP, node1.release(ref, ReleaseRequest.completed()));
        assertEquals(ReleaseOutcome.LOST_OWNERSHIP, node1.release(ref, ReleaseRequest.yielded("PAUSE", "late")));
        assertCounters(ref, 1, 1);
        assertEquals("node-2", hot(ref).getNodeId());
    }

    @Test
    void anExpiredLeaseIsBilledExactlyOncePerReclaimNotOncePerClaimAttempt() {
        TaskRef ref = new TaskRef("ADV", "one-bill");
        node1.enqueue(ref);
        node1.claim(ref);
        killOwnerAndMakeStealable(ref);

        assertEquals(ClaimOutcome.WON, node2.claim(ref));
        assertEquals(1, hot(ref).getRetryCount());

        // Losing claimers must not bill: their UPDATE matches 0 rows, so the CASE never evaluates.
        assertEquals(ClaimOutcome.LOST, node1.claim(ref));
        assertEquals(1, hot(ref).getRetryCount(), "a lost claim race must never move the counter");

        // A won claim always bills, though — even node-2 re-taking its own row. That branch of the
        // eligibility block exists for a node that restarted and lost its in-memory state, which
        // really is a new execution; no won claim is free except one resuming a YIELD.
        assertEquals(ClaimOutcome.WON, node2.claim(ref));
        assertEquals(2, hot(ref).getRetryCount(), "a won claim is billed regardless of who won it");
    }

    @Test
    void aNodeReclaimingItsOwnExpiredLeaseBillsItself() {
        TaskRef ref = new TaskRef("ADV", "self-reclaim");
        node1.enqueue(ref);
        node1.claim(ref);

        // Same node, same row, but the lease lapsed — the execution it was running is just as dead
        // as if another node had taken over, so it is billed the same way. Eligibility here comes
        // from the "CLAIMED AND node_id = me" branch, so no backdating is needed.
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = hot(ref);
            row.setLeaseExpiresAt(Instant.now().minus(Duration.ofSeconds(1)));
            inFlightRepository.save(row);
        });

        assertEquals(ClaimOutcome.WON, node1.claim(ref));
        assertCounters(ref, 1, 1);
    }

    @Test
    void aClaimTimeoutStealBillsTooRegardlessOfWhatTheLeaseSays() {
        TaskRef ref = new TaskRef("ADV", "timeout-live-lease");
        node1.enqueue(ref);
        node1.claim(ref);
        assertEquals(0, hot(ref).getRetryCount());

        // A row can become stealable via the claim-timeout window (updated_at gone quiet) while its
        // lease is still nominally in the future. Keying the bill off the lease would have made this
        // steal free; billing every claim instead makes the rule independent of which of the four
        // windows let the steal through, so no combination of them smuggles a free execution past
        // the budget.
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = hot(ref);
            row.setLeaseExpiresAt(Instant.now().plus(Duration.ofHours(1)));
            row.setUpdatedAt(Instant.now().minus(Duration.ofMinutes(30)));
            inFlightRepository.save(row);
        });

        assertEquals(ClaimOutcome.WON, node2.claim(ref));
        assertEquals(1, hot(ref).getRetryCount(), "the steal starts a new execution, so it is billed");
    }

    @Test
    void repeatedLeaseLossTerminatesTheTaskOnceItsBudgetIsSpent() {
        TaskRef ref = new TaskRef("ADV", "forever");
        node1.enqueue(ref);

        // Nothing here ever releases — every execution's node dies mid-flight, which release()
        // structurally cannot observe (its write is unfenced and returns LOST_OWNERSHIP). Billing
        // at claim is what makes these cost anything at all: ordinals 0..3 are admitted.
        for (int i = 0; i <= 3; i++) {
            assertEquals(ClaimOutcome.WON, node1.claim(ref));
            assertEquals(i, hot(ref).getRetryCount(), "each claim starts an execution and bills for it");
            killOwnerAndMakeStealable(ref);
        }

        // ...and the claim that would start a 5th terminates the task instead. This is the whole
        // point of billing at claim: without it, a task that can never finish inside one lease is
        // reclaimed forever, never reaching a terminal state and never surfacing as a failure.
        assertEquals(ClaimOutcome.EXHAUSTED, node1.claim(ref));

        assertTrue(inFlightRepository.findById(new TaskId("ADV", "forever")).isEmpty(),
                "terminal means the hot row is deleted, same as every other terminal outcome (D28)");
        TmTaskEntity coldRow = taskRepository.findById(new TaskId("ADV", "forever")).orElseThrow();
        assertEquals(TaskStatus.FAILED, coldRow.getStatus(), "and it really is terminal now");
        assertEquals("RETRY_BUDGET_EXHAUSTED", coldRow.getReasonCode());
        assertEquals(3, coldRow.getRetryCount(),
                "the snapshot records the last ordinal that ran, not the one that never started");
    }

    // ---- double / stale release ----------------------------------------

    @Test
    void aSecondReleaseOfTheSameExecutionCannotDoubleBill() {
        TaskRef ref = new TaskRef("ADV", "double-release");
        node1.enqueue(ref);
        node1.claim(ref);

        assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.retry("BOOM", "once")));
        assertCounters(ref, 1, 0);

        // The row is RELEASED now, not CLAIMED — every release path re-fences on status=CLAIMED.
        assertEquals(ReleaseOutcome.LOST_OWNERSHIP, node1.release(ref, ReleaseRequest.retry("BOOM", "again")));
        assertEquals(ReleaseOutcome.LOST_OWNERSHIP, node1.release(ref, ReleaseRequest.yielded("PAUSE", "again")));
        assertCounters(ref, 1, 0);
    }

    @Test
    void renewNeverTouchesEitherCounter() {
        TaskRef ref = new TaskRef("ADV", "renew");
        node1.enqueue(ref);
        node1.claim(ref);
        node1.release(ref, ReleaseRequest.retry("BOOM", "once"));
        node1.claim(ref);
        assertCounters(ref, 1, 1);

        for (int i = 0; i < 5; i++) {
            assertEquals(RenewOutcome.RENEWED, node1.renew(ref));
        }
        assertCounters(ref, 1, 1);
    }

    // ---- budget edges ---------------------------------------------------

    @Test
    void aZeroRetryBudgetAllowsExactlyOneExecution() {
        MultiNodeTaskLedger strict = ledgerFor("node-1", 0);
        TaskRef ref = new TaskRef("ADV", "zero-budget");
        strict.enqueue(ref);

        assertEquals(ClaimOutcome.WON, strict.claim(ref));
        assertEquals(ReleaseOutcome.EXHAUSTED, strict.release(ref, ReleaseRequest.retry("BOOM", "first and last")),
                "max_retries=0 means the initial execution is the only one");
        assertEquals(TaskStatus.FAILED, cold(ref).getStatus());
        assertEquals(0, cold(ref).getRetryCount(), "no retries were ever burned");
        assertEquals(1, cold(ref).getAttemptCount());
    }

    @Test
    void aYieldAtTheEdgeOfTheBudgetDoesNotSpendTheLastRetry() {
        TaskRef ref = new TaskRef("ADV", "yield-edge");
        node1.enqueue(ref);

        // Spend the budget down to its last unit: retry_count == max_retries == 3, i.e. the fourth
        // and final execution of this attempt is the one now running.
        for (int i = 0; i < 3; i++) {
            node1.claim(ref);
            assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.retry("BOOM", "fail " + i)));
        }
        assertEquals(ClaimOutcome.WON, node1.claim(ref));
        assertCounters(ref, 1, 3);

        // A yield here must not tip it over — nor must an unbounded number of them. Resuming a
        // yield is the one free claim, so a pause/resume loop can never bankrupt a task.
        for (int i = 0; i < 10; i++) {
            assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.yielded("PAUSE", "cycle " + i)));
            assertEquals(TaskStatus.YIELDED, hot(ref).getStatus());
            assertFalse(hot(ref).getNextRetryAt().isAfter(Instant.now()), "and never gates the next claim");
            assertEquals(ClaimOutcome.WON, node1.claim(ref));
            assertCounters(ref, 1, 3);
        }

        // The execution it kept handing back is still the last one it gets.
        assertEquals(ReleaseOutcome.EXHAUSTED, node1.release(ref, ReleaseRequest.retry("BOOM", "the last one")));
    }

    @Test
    void terminalSnapshotsRecordRetriesBurnedNotThePhantomNextOne() {
        TaskRef completed = new TaskRef("ADV", "snap-ok");
        node1.enqueue(completed);
        for (int i = 0; i < 2; i++) {
            node1.claim(completed);
            node1.release(completed, ReleaseRequest.retry("BOOM", "fail " + i));
        }
        node1.claim(completed);
        assertEquals(ReleaseOutcome.OK, node1.release(completed, ReleaseRequest.completed()));
        assertEquals(1, cold(completed).getAttemptCount());
        assertEquals(2, cold(completed).getRetryCount(), "two retries were burned before it succeeded");

        TaskRef exhausted = new TaskRef("ADV", "snap-dead");
        node1.enqueue(exhausted);
        for (int i = 0; i < 3; i++) {
            node1.claim(exhausted);
            node1.release(exhausted, ReleaseRequest.retry("BOOM", "fail " + i));
        }
        node1.claim(exhausted);
        assertEquals(ReleaseOutcome.EXHAUSTED, node1.release(exhausted, ReleaseRequest.retry("BOOM", "final")));
        assertEquals(3, cold(exhausted).getRetryCount(),
                "3 retries actually happened — not the 4th, which was only ever a candidate");
        // The snapshotted max_retries is the *row-level* cap, left deliberately unbounded so the
        // wired RetryPolicy is the real gate (D16, TmTaskEntities.UNBOUNDED_MAX_RETRIES). It is
        // therefore NOT a record of the budget that actually applied — reading the cold row alone
        // tells you retries burned, not retries allowed. Asserted so the reconciliation stays
        // visible while D33's "where does max_retries live" open item is unresolved.
        assertEquals(Integer.MAX_VALUE, cold(exhausted).getMaxRetries());
    }

    // ---- rerun boundary --------------------------------------------------

    @Test
    void enqueueOfALiveTaskNeverRewindsTheCounters() {
        TaskRef ref = new TaskRef("ADV", "re-enqueue");
        node1.enqueue(ref);
        node1.claim(ref);
        node1.release(ref, ReleaseRequest.retry("BOOM", "once"));
        assertCounters(ref, 1, 0);

        // enqueue() is documented idempotent; a duplicate must not resurrect a fresh (1, -1) row and
        // hand the task an unbounded budget by the back door.
        node1.enqueue(ref);
        node2.enqueue(ref);
        assertCounters(ref, 1, 0);
        assertEquals(TaskStatus.RELEASED, hot(ref).getStatus(), "and must not un-release it either");
    }

    @Test
    void rerunRefusesALiveTaskSoTheOuterCounterCannotBeInflatedAtWill() {
        TaskRef ref = new TaskRef("ADV", "rerun-live");
        node1.enqueue(ref);
        node1.claim(ref);
        node1.release(ref, ReleaseRequest.yielded("PAUSE", "yielded, still alive"));

        // A yielded task is back to QUEUED and still in the ordinary cycle — rerun is not its owner.
        assertThrows(IllegalStateException.class, () -> node1.rerun(ref));
        assertCounters(ref, 1, 0);
    }

    @Test
    void countersOfAnUnknownTaskAreNoneRatherThanAFabricatedFirstAttempt() {
        assertEquals(AttemptCounters.NONE, node1.counters(new TaskRef("ADV", "never-existed")));
        assertEquals(0, AttemptCounters.NONE.attemptCount());
        assertEquals(0, AttemptCounters.NONE.retryCount());
    }

    @Test
    void theBackoffCurveRestartsWithANewAttemptInsteadOfInheritingTheOldOne() {
        // base=1s, cap=1h: the curve is observable rather than collapsed to 1ms.
        MultiNodeTaskLedger slow = ledgerFor("node-1",
                new ExponentialBackoffRetryPolicy(3, Duration.ofSeconds(1), Duration.ofHours(1)));
        TaskRef ref = new TaskRef("ADV", "backoff");
        slow.enqueue(ref);

        for (int i = 0; i < 3; i++) {
            slow.claim(ref);
            slow.release(ref, ReleaseRequest.retry("BOOM", "fail " + i));
        }
        // retry 3 of attempt 1 -> 1s * 2^2 = 4s.
        Duration deepDelay = Duration.between(Instant.now(), hot(ref).getNextRetryAt());
        assertTrue(deepDelay.toMillis() > 3_000, "backoff has grown by the third retry, was " + deepDelay);

        slow.claim(ref);
        slow.release(ref, ReleaseRequest.retry("BOOM", "final"));   // exhausts
        slow.rerun(ref);
        slow.claim(ref);
        slow.release(ref, ReleaseRequest.retry("BOOM", "attempt 2, retry 1"));

        // retry 1 of attempt 2 -> back to the base delay. Keyed off attempt_count it would still
        // be deep in the curve, which is the inflation D33 removes.
        Duration freshDelay = Duration.between(Instant.now(), hot(ref).getNextRetryAt());
        assertTrue(freshDelay.toMillis() <= 1_000,
                "a new attempt must restart the curve at base, was " + freshDelay);
    }

    // ---- the load-bearing history-key claim -------------------------------

    @Test
    void historyFromBeforeAndAfterARerunCoexistsInsteadOfBeingOverwritten() {
        AttemptHistoryWriter writer = new AttemptHistoryWriter(attemptHistoryRepository);
        TaskRef ref = new TaskRef("ADV", "history-key");
        node1.enqueue(ref);

        // Attempt 1: executions (1,0) and (1,1).
        for (int retry = 0; retry <= 1; retry++) {
            node1.claim(ref);
            AttemptCounters c = node1.counters(ref);
            assertEquals(new AttemptCounters(1, retry), c);
            writer.record(new HistoryEvent(ref, c.attemptCount(), c.retryCount(), HistoryEventType.STARTED,
                    null, Instant.now(), "attempt1-exec" + retry));
            node1.release(ref, ReleaseRequest.retry("BOOM", "fail"));
        }
        node1.claim(ref);
        node1.release(ref, ReleaseRequest.failedTerminal("DEAD", "give up"));

        node1.rerun(ref);
        node1.claim(ref);
        AttemptCounters afterRerun = node1.counters(ref);
        assertEquals(new AttemptCounters(2, 0), afterRerun);
        writer.record(new HistoryEvent(ref, afterRerun.attemptCount(), afterRerun.retryCount(),
                HistoryEventType.STARTED, null, Instant.now(), "attempt2-exec0"));

        // This is the whole reason the PK had to widen with the reset: under the old single-counter
        // key, attempt 2's reset-to-0 execution would have had the same key as (1, 0) and silently
        // overwritten it. Both must survive, with their own details.
        List<TmAttemptHistoryEntity> rows = attemptHistoryRepository.findAll().stream()
                .filter(r -> r.getTaskId().equals("history-key"))
                .toList();
        assertEquals(3, rows.size(), "three distinct executions, three rows");
        assertEquals(1, rows.stream().filter(r -> r.getAttemptCount() == 1 && r.getRetryCount() == 0).count());
        assertEquals(1, rows.stream().filter(r -> r.getAttemptCount() == 1 && r.getRetryCount() == 1).count());
        assertEquals(1, rows.stream().filter(r -> r.getAttemptCount() == 2 && r.getRetryCount() == 0).count());

        // node_id is stamped per event (AttemptHistoryWriter's STARTED insert deliberately writes
        // no detail), so it is what proves the (1, 0) row is still its own row rather than attempt
        // 2's write landing on top of it.
        TmAttemptHistoryEntity preRerun = rows.stream()
                .filter(r -> r.getAttemptCount() == 1 && r.getRetryCount() == 0).findFirst().orElseThrow();
        assertEquals("attempt1-exec0", preRerun.getNodeId(),
                "the pre-rerun row must still be its own row, not overwritten by attempt 2's (2, 0) write");
        assertEquals("attempt2-exec0", rows.stream()
                .filter(r -> r.getAttemptCount() == 2 && r.getRetryCount() == 0).findFirst().orElseThrow().getNodeId());
    }

    // ---- cross-node visibility -------------------------------------------

    @Test
    void bothCountersAreReadTheSameWayByEveryNode() {
        TaskRef ref = new TaskRef("ADV", "cross-node");
        node1.enqueue(ref);
        node1.claim(ref);
        node1.release(ref, ReleaseRequest.retry("BOOM", "once"));
        makeStealable(ref);
        assertEquals(ClaimOutcome.WON, node2.claim(ref));

        // Counters live on the row, not in either engine's memory.
        assertEquals(new AttemptCounters(1, 1), node1.counters(ref));
        assertEquals(new AttemptCounters(1, 1), node2.counters(ref));
    }

    // ---- genuine concurrency ----------------------------------------------

    /**
     * Runs outside the test-managed transaction so each thread's commit is real and
     * visible to the others — otherwise they would all sit behind the same uncommitted
     * connection and the race would not exist.
     *
     * <p>Every {@code claim} is therefore wrapped in a transaction of its own here.
     * {@code ClaimStrategy} opens none: the caller owns that boundary (in production the
     * engine does, via {@code TransactionRunner}), which is what lets a terminal transition
     * and the consumer's {@code onTerminal} commit as one unit.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrentReclaimersBillTheDeadExecutionExactlyOnce() throws Exception {
        TaskRef ref = new TaskRef("ADV", "race");
        try {
            node1.enqueue(ref);
            transactionTemplate.executeWithoutResult(status -> node1.claim(ref));
            killOwnerAndMakeStealable(ref);

            int contenders = 8;
            List<MultiNodeTaskLedger> nodes = java.util.stream.IntStream.range(0, contenders)
                    .mapToObj(i -> ledgerFor("racer-" + i, 3))
                    .toList();

            ExecutorService pool = Executors.newFixedThreadPool(contenders);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger won = new AtomicInteger();
            try {
                List<Future<?>> futures = new java.util.ArrayList<>();
                for (MultiNodeTaskLedger n : nodes) {
                    futures.add(pool.submit(() -> {
                        go.await();
                        // Each racer supplies its own boundary, exactly as the engine would.
                        ClaimOutcome outcome = new TransactionTemplate(transactionManager)
                                .execute(status -> n.claim(ref));
                        if (outcome == ClaimOutcome.WON) {
                            won.incrementAndGet();
                        }
                        return null;
                    }));
                }
                go.countDown();
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            assertEquals(1, won.get(), "claim is the only cross-node gate — exactly one winner");
            assertEquals(1, hot(ref).getRetryCount(),
                    "the dead execution is billed once, by the winner — losing claimers match 0 rows "
                            + "and so never evaluate the CASE");
            assertEquals(1, hot(ref).getAttemptCount());
            assertNotNull(hot(ref).getNodeId());
        } finally {
            // This test commits for real, so it cleans up after itself.
            transactionTemplate.executeWithoutResult(status -> {
                inFlightRepository.deleteByTaskTypeAndTaskId(ref.taskType(), ref.taskId());
                taskRepository.findById(new TaskId(ref.taskType(), ref.taskId())).ifPresent(taskRepository::delete);
            });
        }
    }
}
