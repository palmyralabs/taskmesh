package com.palmyralabs.taskmesh.max;
import com.palmyralabs.taskmesh.max.entity.PartitionedTaskId;
import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.entity.TaskStatus;
import com.palmyralabs.taskmesh.max.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.max.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.AttemptCounters;
import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises PartitionedTaskLedger's 3-window (reclaim/affinity/timeout) claim
 * model (D27) against real H2, now split across the cold {@code tm_task}
 * table and the hot {@code tm_in_flight} table (D-in_flight) — the SQL was
 * hand-translated from palmyra76/workflow's fixed claim UPDATE and had never
 * actually run before this test existed. {@code partitionKey} is required at
 * this tier (D32) — every {@link TaskRef} below carries one.
 */
@DataJpaTest
class PartitionedTaskLedgerTest {

    private static final String P = "P1";

    @Autowired
    private TmTaskRepository taskRepository;

    @Autowired
    private TmInFlightRepository inFlightRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private PartitionedTaskLedger node1;
    private PartitionedTaskLedger node2;
    private NoopBacklogPromoter backlogPromoter;

    @BeforeEach
    void setUp() {
        transactionTemplate = new TransactionTemplate(transactionManager);
        backlogPromoter = new NoopBacklogPromoter(taskRepository, inFlightRepository, transactionTemplate);
        node1 = ledgerFor("node-1");
        node2 = ledgerFor("node-2");
    }

    /** This tier's enqueue() only writes tm_task — mirrors what the engine does by also calling onEnqueue. */
    private void enqueueAndPromote(PartitionedTaskLedger ledger, TaskRef taskRef, String nodeId) {
        ledger.enqueue(taskRef);
        backlogPromoter.onEnqueue(taskRef, nodeId);
    }

    // leaseDuration=5min, reclaimWindow=30s (write-min only, never eligibility),
    // affinityWindow=60s, claimTimeout=10min, reservationWindow=60s.
    private PartitionedTaskLedger ledgerFor(String nodeId) {
        return new PartitionedTaskLedger(taskRepository, inFlightRepository, transactionTemplate, () -> nodeId,
                new ExponentialBackoffRetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1)),
                Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ofSeconds(60), Duration.ofMinutes(10),
                Duration.ofSeconds(60));
    }

    private TmInFlightEntity inFlightRow(TaskRef taskRef) {
        return inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElseThrow();
    }

    private TmTaskEntity taskRow(TaskRef taskRef) {
        return taskRepository.findById(new PartitionedTaskId(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey())).orElseThrow();
    }

    /**
     * Simulates "this hot row is from way in the past, ready to be picked up
     * now" — backdates the hot row's updatedAt (which drives the reservation/
     * affinity/claim-timeout windows) and clears its nextRetryAt (rather than
     * relying on the retry policy's real, tiny backoff window elapsing in
     * wall-clock time, which made an earlier version of this suite flaky).
     */
    private void backdateUpdatedAt(TaskRef taskRef, Instant when) {
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = inFlightRow(taskRef);
            row.setUpdatedAt(when);
            row.setNextRetryAt(Instant.EPOCH);   // "visible since forever" - the gate is a timestamp now, never null
            inFlightRepository.save(row);
        });
    }

    /** The owner died mid-execution: lease lapsed, and it never released. Re-claimable by itself. */
    private void killOwner(TaskRef taskRef) {
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = inFlightRow(taskRef);
            row.setLeaseExpiresAt(Instant.now().minus(Duration.ofMinutes(1)));
            row.setNextRetryAt(Instant.EPOCH);   // "visible since forever" - the gate is a timestamp now, never null
            inFlightRepository.save(row);
        });
    }

    /** Expires the current lease in place, standing in for "the owning node died mid-execution". */
    private void expireLease(TaskRef taskRef) {
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = inFlightRow(taskRef);
            row.setLeaseExpiresAt(Instant.now().minus(Duration.ofMinutes(1)));
            inFlightRepository.save(row);
        });
    }

    @Test
    void claimWinsForFirstNodeAndLosesForSecond() {
        TaskRef taskRef = new TaskRef("T", "1", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef));
    }

    @Test
    void completedReleaseMakesTaskUnclaimableAgainAndDeletesHotRow() {
        TaskRef taskRef = new TaskRef("T", "2", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef);

        assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.completed()));
        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef));

        assertEquals(TaskStatus.COMPLETED, taskRow(taskRef).getStatus());
        assertTrue(inFlightRepository.findById(new TaskId("T", "2")).isEmpty(), "terminal release must delete the hot row");
    }

    @Test
    void retryReleaseSetsReleasedStatusAndSameNodeCanReclaimWithinAffinity() {
        TaskRef taskRef = new TaskRef("T", "3", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef);

        assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.retry("TRANSIENT", "boom")));

        TmInFlightEntity row = inFlightRow(taskRef);
        assertEquals(TaskStatus.RELEASED, row.getStatus());
        assertEquals("node-1", row.getNodeId(), "node_id must NOT be cleared on requeue — it's the affinity key");
        assertEquals(TaskStatus.QUEUED, taskRow(taskRef).getStatus(), "cold row stays QUEUED throughout — CLAIMED/RELEASED never appear there");

        // Still within the affinity window (60s) — same node can reclaim immediately.
        assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
    }

    @Test
    void retryReleaseBlocksOtherNodesWithinAffinityWindow() {
        TaskRef taskRef = new TaskRef("T", "4", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef);
        node1.release(taskRef, ReleaseRequest.retry("TRANSIENT", "boom"));

        // Fresh RELEASED row, still within the 60s affinity window — node2 must NOT win.
        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef));
    }

    @Test
    void retryReleaseAllowsOtherNodesOnceAffinityExpires() {
        TaskRef taskRef = new TaskRef("T", "5", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef);
        node1.release(taskRef, ReleaseRequest.retry("TRANSIENT", "boom"));

        backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(5))); // past the 60s affinity window

        assertEquals(ClaimOutcome.WON, node2.claim(taskRef));
    }

    @Test
    void staleClaimIsStolenAfterClaimTimeout() {
        TaskRef taskRef = new TaskRef("T", "6", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef); // simulates node-1, which then "crashes" — never releases

        backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(20))); // past the 10min claimTimeout

        assertEquals(ClaimOutcome.WON, node2.claim(taskRef));
    }

    @Test
    void freshClaimIsNotStealableBeforeClaimTimeout() {
        TaskRef taskRef = new TaskRef("T", "7", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef);

        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef));
    }

    @Test
    void exhaustionEscalatesToFailedAndDeletesHotRow() {
        TaskRef taskRef = new TaskRef("T", "8", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        // max_retries=3 on the retry policy: the 1st, 2nd and 3rd RETRY releases stay within
        // budget; the 4th execution's failure trips exhaustion (D33 preserves the old total).
        for (int i = 0; i < 3; i++) {
            node1.claim(taskRef);
            assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.retry("BOOM", "fail " + i)));
            backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(5)));
        }
        node1.claim(taskRef);
        assertEquals(ReleaseOutcome.EXHAUSTED, node1.release(taskRef, ReleaseRequest.retry("BOOM", "final fail")));

        assertEquals(TaskStatus.FAILED, taskRow(taskRef).getStatus());
        assertTrue(inFlightRepository.findById(new TaskId("T", "8")).isEmpty(), "exhaustion is terminal — hot row must be deleted");
    }

    @Test
    void pullPendingMirrorsClaimEligibilityForTheQueryingNode() {
        TaskRef queued = new TaskRef("T", "9", P);
        TaskRef claimedByNode1Fresh = new TaskRef("T", "10", P);
        TaskRef claimedByNode1Stale = new TaskRef("T", "11", P);
        TaskRef releasedByNode1Fresh = new TaskRef("T", "12", P);
        TaskRef releasedByNode1Stale = new TaskRef("T", "13", P);

        for (TaskRef ref : List.of(queued, claimedByNode1Fresh, claimedByNode1Stale, releasedByNode1Fresh, releasedByNode1Stale)) {
            enqueueAndPromote(node1, ref, "node-1");
        }

        node1.claim(claimedByNode1Fresh);
        node1.claim(claimedByNode1Stale);
        backdateUpdatedAt(claimedByNode1Stale, Instant.now().minus(Duration.ofMinutes(20)));

        node1.claim(releasedByNode1Fresh);
        node1.release(releasedByNode1Fresh, ReleaseRequest.retry("X", "x"));

        node1.claim(releasedByNode1Stale);
        node1.release(releasedByNode1Stale, ReleaseRequest.retry("X", "x"));
        backdateUpdatedAt(releasedByNode1Stale, Instant.now().minus(Duration.ofMinutes(5)));

        // queued was enqueued by node1 (reserved to it) — age it past the reservation window so node2 can see it.
        backdateUpdatedAt(queued, Instant.now().minus(Duration.ofMinutes(5)));

        List<TaskRef> node2Pending = node2.pullPending(Instant.now(), 100);

        assertTrue(node2Pending.contains(queued), "QUEUED past its reservation window is open to any node");
        assertTrue(node2Pending.contains(claimedByNode1Stale), "stale claim (past claimTimeout) must be stealable");
        assertTrue(node2Pending.contains(releasedByNode1Stale), "released past affinity must be open to any node");
        assertFalse(node2Pending.contains(claimedByNode1Fresh), "fresh claim by another node must not be offered");
        assertFalse(node2Pending.contains(releasedByNode1Fresh), "released within affinity must not be offered to another node");
    }

    @Test
    void enqueueIsIdempotentAndDoesNotResetAnInProgressRow() {
        TaskRef taskRef = new TaskRef("T", "14", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef);

        enqueueAndPromote(node1, taskRef, "node-1"); // re-enqueue while claimed — must not reset status/ownership

        TmInFlightEntity row = inFlightRow(taskRef);
        assertEquals(TaskStatus.CLAIMED, row.getStatus());
        assertEquals("node-1", row.getNodeId());
        assertEquals(TaskStatus.QUEUED, taskRow(taskRef).getStatus());
    }

    @Test
    void enqueuePlusNoopPromoterCreatesBothColdAndHotRowsTogether() {
        // Unlike taskmesh-simple/-pro, this tier's enqueue() alone only writes tm_task —
        // NoopBacklogPromoter.onEnqueue (called by the engine right after) is what
        // reproduces the historical eager-pair behavior; see enqueueAndPromote().
        TaskRef taskRef = new TaskRef("T", "15", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        assertEquals(TaskStatus.QUEUED, taskRow(taskRef).getStatus());
        Optional<TmInFlightEntity> hot = inFlightRepository.findById(new TaskId("T", "15"));
        assertTrue(hot.isPresent(), "enqueue+onEnqueue must create the hot row eagerly, not lazily on first claim");
        assertEquals(TaskStatus.QUEUED, hot.get().getStatus(), "the hot row is reserved (QUEUED) to the enqueuing node");
        assertEquals("node-1", hot.get().getNodeId(), "reservation keys on the enqueuing node's id");
        assertEquals(P, hot.get().getPartitionKey(), "partitionKey must propagate onto the hot row too");
    }

    @Test
    void reservedQueuedIsNotStealableByAnotherNodeWithinReservationWindow() {
        TaskRef taskRef = new TaskRef("T", "16", P);
        enqueueAndPromote(node1, taskRef, "node-1"); // QUEUED, reserved to node-1

        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef), "a fresh reservation must not be stealable by another node");
        assertEquals(ClaimOutcome.WON, node1.claim(taskRef), "the reserving node claims its own work immediately");
    }

    @Test
    void reservedQueuedBecomesStealableAfterReservationWindow() {
        TaskRef taskRef = new TaskRef("T", "17", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(5))); // past the 60s reservation window

        assertEquals(ClaimOutcome.WON, node2.claim(taskRef), "past the reservation window, any node may steal a still-QUEUED row");
    }

    @Test
    void rerunRecreatesTheHotRowForATerminallyFailedTaskAndMakesItClaimableAgain() {
        TaskRef taskRef = new TaskRef("T", "18", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef);
        node1.release(taskRef, ReleaseRequest.failedTerminal("BOOM", "dead")); // terminal — deletes the hot row

        // attempt_count on the cold snapshot = the attempt that just failed terminally: attempt 1.
        assertEquals(TaskStatus.FAILED, taskRow(taskRef).getStatus());
        assertEquals(1, taskRow(taskRef).getAttemptCount(), "cold snapshot holds the attempt that just ran");
        assertEquals(0, taskRow(taskRef).getRetryCount(), "it failed terminally on its first execution \u2014 no retries burned");
        assertTrue(inFlightRepository.findById(new TaskId("T", "18")).isEmpty(), "terminal deleted the hot row");

        node1.rerun(taskRef);

        TmInFlightEntity hot = inFlightRow(taskRef);
        assertEquals(TaskStatus.QUEUED, hot.getStatus(), "rerun recreates the hot row, reserved and QUEUED");
        assertEquals(2, hot.getAttemptCount(), "seeded from cold's attempt_count (1) + 1 — the next attempt is #2");
        assertEquals(-1, hot.getRetryCount(),
                "and the inner counter restarts for the new attempt — the first claim bills it to 0");
        assertEquals(TaskStatus.QUEUED, taskRow(taskRef).getStatus(), "cold row forced back to QUEUED");
        assertEquals(ClaimOutcome.WON, node1.claim(taskRef), "reran task is claimable again");
    }

    @Test
    void rerunThrowsWhenAHotRowAlreadyExists() {
        TaskRef taskRef = new TaskRef("T", "24", P);
        enqueueAndPromote(node1, taskRef, "node-1");
        node1.claim(taskRef); // hot row now CLAIMED, still exists

        assertThrows(IllegalStateException.class, () -> node1.rerun(taskRef),
                "rerun must refuse a task that's still alive in the ordinary claim/retry cycle");
        assertEquals(TaskStatus.CLAIMED, inFlightRow(taskRef).getStatus(), "the live hot row must be untouched");
    }

    @Test
    void rerunAfterExhaustionOpensANewAttemptWithAFullFreshRetryBudget() {
        TaskRef taskRef = new TaskRef("T", "22", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        // Burn the whole budget (max_retries=3): 3 RETRY releases stay within it, the 4th trips it.
        for (int i = 0; i < 3; i++) {
            node1.claim(taskRef);
            assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.retry("BOOM", "fail " + i)));
            backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(5)));
        }
        node1.claim(taskRef);
        assertEquals(ReleaseOutcome.EXHAUSTED, node1.release(taskRef, ReleaseRequest.retry("BOOM", "final fail")));

        assertEquals(1, taskRow(taskRef).getAttemptCount(), "automatic retry never moves the outer counter \u2014 still attempt 1");
        assertEquals(3, taskRow(taskRef).getRetryCount(), "the cold snapshot records the 3 retries actually burned");
        assertTrue(inFlightRepository.findById(new TaskId("T", "22")).isEmpty(), "exhaustion deleted the hot row");

        node1.rerun(taskRef);

        // The whole point of the D33 split: a task that exhausted its retries can be re-run and
        // gets a full fresh budget, instead of re-exhausting on its first failure.
        assertEquals(new AttemptCounters(2, -1), node1.counters(taskRef),
                "rerun opens attempt 2 with the inner counter reset to 'no execution yet'");
        assertEquals(2, inFlightRow(taskRef).getAttemptCount());
        assertEquals(-1, inFlightRow(taskRef).getRetryCount());

        for (int i = 0; i < 3; i++) {
            assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
            assertEquals(i, inFlightRow(taskRef).getRetryCount(), "each claim of attempt 2 bills one unit");
            assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.retry("BOOM", "again " + i)),
                    "attempt 2 must get the full retry budget, not inherit attempt 1's exhausted one");
            backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(5)));
        }
        assertEquals(2, inFlightRow(taskRef).getAttemptCount(), "the outer counter stays put across a whole retry run");
        node1.claim(taskRef);
        assertEquals(ReleaseOutcome.EXHAUSTED, node1.release(taskRef, ReleaseRequest.retry("BOOM", "done")),
                "and the fresh budget is the same size, not unbounded");
    }

    @Test
    void aTaskWhoseExecutionsKeepDyingIsTerminatedAtClaimTime() {
        TaskRef taskRef = new TaskRef("T", "36", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        // Nothing here ever releases: every execution's node dies mid-flight, which is precisely
        // the case release() can never observe (its write is unfenced and returns LOST_OWNERSHIP).
        // max_retries=3 -> ordinals 0..3 are admitted, and the claim that would start ordinal 4
        // terminates the task instead of handing it over.
        for (int i = 0; i <= 3; i++) {
            assertEquals(ClaimOutcome.WON, node1.claim(taskRef), "execution " + i + " must be admitted");
            assertEquals(i, inFlightRow(taskRef).getRetryCount());
            killOwner(taskRef);
        }

        assertEquals(ClaimOutcome.EXHAUSTED, node1.claim(taskRef),
                "the budget is spent, so the claim terminates the task rather than starting a 5th execution");

        assertTrue(inFlightRepository.findById(new TaskId("T", "36")).isEmpty(),
                "exhaustion is terminal — the hot row must be deleted, exactly as on the release path");
        TmTaskEntity coldRow = taskRow(taskRef);
        assertEquals(TaskStatus.FAILED, coldRow.getStatus());
        assertEquals("RETRY_BUDGET_EXHAUSTED", coldRow.getReasonCode());
        assertEquals(1, coldRow.getAttemptCount());
        assertEquals(3, coldRow.getRetryCount(),
                "the snapshot records the last ordinal that actually ran, not the one that never started");
    }

    @Test
    void totalExecutionBudgetIsUnchangedByTheCounterSplit() {
        TaskRef taskRef = new TaskRef("T", "33", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        // max_retries=3 must still allow exactly 4 executions \u2014 the same total the old
        // pre-increment `attempt_count > max_attempts` check gave. The rename must not shift it by one.
        int executions = 0;
        ReleaseOutcome outcome;
        while (true) {
            assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
            executions++;
            outcome = node1.release(taskRef, ReleaseRequest.retry("BOOM", "fail " + executions));
            if (outcome != ReleaseOutcome.OK) {
                break;
            }
            backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(5)));
        }

        assertEquals(ReleaseOutcome.EXHAUSTED, outcome);
        assertEquals(4, executions, "max_retries=3 allows the initial execution plus 3 retries");
    }

    @Test
    void yieldReturnsWorkWithoutConsumingBudgetOrSettingABackoffGate() {
        TaskRef taskRef = new TaskRef("T", "34", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        // Burn one retry first, so "unchanged" is a real observation rather than 0 == 0.
        assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
        assertEquals(0, inFlightRow(taskRef).getRetryCount(), "the first claim bills the counter up to 0 (D33)");
        assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.retry("BOOM", "once")));
        assertEquals(0, inFlightRow(taskRef).getRetryCount(), "release does not bill — claim does");
        assertNotNull(inFlightRow(taskRef).getNextRetryAt(), "but a retry does set the backoff gate");

        assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
        assertEquals(1, inFlightRow(taskRef).getRetryCount(), "the retry's own claim bills it");
        assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.yielded("PAUSE_REQUESTED", "cooperative pause")));

        TmInFlightEntity hot = inFlightRow(taskRef);
        assertEquals(TaskStatus.YIELDED, hot.getStatus(), "a yield parks the row in its own status, not QUEUED");
        assertEquals(1, hot.getAttemptCount(), "a yield is not a new attempt");
        assertEquals(1, hot.getRetryCount(), "a yield consumes no retry budget");
        assertFalse(hot.getNextRetryAt().isAfter(Instant.now()), "a yield leaves no future backoff gate — it must be immediately claimable");
        assertEquals(ClaimOutcome.WON, node1.claim(taskRef), "immediately re-claimable after a yield");
        assertEquals(1, inFlightRow(taskRef).getRetryCount(),
                "and resuming a yield is the one claim that bills nothing — it is the same execution");
    }

    @Test
    void reclaimingAnExpiredLeaseBillsTheExecutionThatSilentlyDied() {
        TaskRef taskRef = new TaskRef("T", "35", P);
        enqueueAndPromote(node1, taskRef, "node-1");

        assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
        assertEquals(0, inFlightRow(taskRef).getRetryCount(), "an ordinary claim bills nothing \u2014 the first execution is retry 0");

        // node-1 dies mid-execution: its own release is unfenced by construction, so it can never
        // record anything. Only the reclaiming node can bill the execution that died.
        expireLease(taskRef);
        backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(30)));
        assertEquals(ClaimOutcome.WON, node2.claim(taskRef));

        assertEquals(1, inFlightRow(taskRef).getRetryCount(),
                "without this, a task that can never finish inside one lease retries forever, "
                        + "never reaching a terminal state and never surfacing as a failure");
        assertEquals(1, inFlightRow(taskRef).getAttemptCount(), "a reclaim is still the same attempt");
    }


    @Test
    void rerunIsANoOpForANeverEnqueuedTask() {
        TaskRef unknown = new TaskRef("T", "19", P);
        node1.rerun(unknown);

        assertTrue(inFlightRepository.findById(new TaskId("T", "19")).isEmpty(),
                "rerun must not fabricate a hot row without a cold ledger row (pair invariant)");
    }

    // ---- D32: partitionKey is required, hard-fail (not a default bucket) on missing ----

    @Test
    void enqueueThrowsOnMissingPartitionKey() {
        TaskRef noPartition = new TaskRef("T", "20");
        assertThrows(IllegalArgumentException.class, () -> node1.enqueue(noPartition));
    }

    @Test
    void rerunThrowsOnMissingPartitionKey() {
        TaskRef noPartition = new TaskRef("T", "21");
        assertThrows(IllegalArgumentException.class, () -> node1.rerun(noPartition));
    }

    // NOTE: (taskType, taskId) must map to exactly one, stable partitionKey — that's a
    // caller contract (D32: "partitionKey must be stable across repeated enqueue calls
    // for the same task_id"), not something this ledger enforces. tm_task's PK technically
    // admits two different partitionKey values for the same (taskType, taskId), but
    // tm_in_flight's PK does NOT include partitionKey (D32 — deliberately unpartitioned),
    // so a caller violating that contract gets an inconsistent, unsupported state (the
    // second enqueue's hot row silently no-ops against NoopBacklogPromoter's "only if
    // absent" guard). Not covered by a test — it's a documented caller responsibility,
    // not a scenario this ledger is designed to handle gracefully.
}
