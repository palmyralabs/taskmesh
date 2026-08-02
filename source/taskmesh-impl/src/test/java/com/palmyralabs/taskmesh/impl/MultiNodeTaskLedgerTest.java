package com.palmyralabs.taskmesh.impl;
import com.palmyralabs.taskmesh.impl.entity.TaskId;
import com.palmyralabs.taskmesh.impl.entity.TaskStatus;
import com.palmyralabs.taskmesh.impl.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.impl.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.impl.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.impl.repository.TmTaskRepository;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises MultiNodeTaskLedger's 3-window (reclaim/affinity/timeout) claim
 * model (D27) against real H2, now split across the cold {@code tm_task}
 * table and the hot {@code tm_in_flight} table (D-in_flight) — the SQL was
 * hand-translated from palmyra76/workflow's fixed claim UPDATE and had never
 * actually run before this test existed.
 */
@DataJpaTest
class MultiNodeTaskLedgerTest {

    @Autowired
    private TmTaskRepository taskRepository;

    @Autowired
    private TmInFlightRepository inFlightRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private MultiNodeTaskLedger node1;
    private MultiNodeTaskLedger node2;

    @BeforeEach
    void setUp() {
        transactionTemplate = new TransactionTemplate(transactionManager);
        node1 = ledgerFor("node-1");
        node2 = ledgerFor("node-2");
    }

    // leaseDuration=5min, reclaimWindow=30s (write-min only, never eligibility),
    // affinityWindow=60s, claimTimeout=10min.
    private MultiNodeTaskLedger ledgerFor(String nodeId) {
        return new MultiNodeTaskLedger(taskRepository, inFlightRepository, transactionTemplate, () -> nodeId,
                new ExponentialBackoffRetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1)),
                Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ofSeconds(60), Duration.ofMinutes(10));
    }

    private TmInFlightEntity inFlightRow(TaskRef taskRef) {
        return inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElseThrow();
    }

    private TmTaskEntity taskRow(TaskRef taskRef) {
        return taskRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElseThrow();
    }

    /**
     * Simulates "this hot row is from way in the past, ready to be picked up
     * now" — also clears the cold row's nextRetryAt (rather than relying on
     * the retry policy's real, tiny backoff window elapsing in wall-clock
     * time, which made an earlier version of this suite flaky).
     */
    private void backdateUpdatedAt(TaskRef taskRef, Instant when) {
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = inFlightRow(taskRef);
            row.setUpdatedAt(when);
            inFlightRepository.save(row);

            TmTaskEntity task = taskRow(taskRef);
            task.setNextRetryAt(null);
            taskRepository.save(task);
        });
    }

    @Test
    void claimWinsForFirstNodeAndLosesForSecond() {
        TaskRef taskRef = new TaskRef("T", "1");
        node1.enqueue(taskRef);

        assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef));
    }

    @Test
    void completedReleaseMakesTaskUnclaimableAgainAndDeletesHotRow() {
        TaskRef taskRef = new TaskRef("T", "2");
        node1.enqueue(taskRef);
        node1.claim(taskRef);

        assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.completed()));
        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef));

        assertEquals(TaskStatus.COMPLETED, taskRow(taskRef).getStatus());
        assertTrue(inFlightRepository.findById(new TaskId("T", "2")).isEmpty(), "terminal release must delete the hot row");
    }

    @Test
    void requeueReleaseSetsReleasedStatusAndSameNodeCanReclaimWithinAffinity() {
        TaskRef taskRef = new TaskRef("T", "3");
        node1.enqueue(taskRef);
        node1.claim(taskRef);

        assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.requeue("TRANSIENT", "boom")));

        TmInFlightEntity row = inFlightRow(taskRef);
        assertEquals(TaskStatus.RELEASED, row.getStatus());
        assertEquals("node-1", row.getNodeId(), "node_id must NOT be cleared on requeue — it's the affinity key");
        assertEquals(TaskStatus.QUEUED, taskRow(taskRef).getStatus(), "cold row stays QUEUED throughout — CLAIMED/RELEASED never appear there");

        // Still within the affinity window (60s) — same node can reclaim immediately.
        assertEquals(ClaimOutcome.WON, node1.claim(taskRef));
    }

    @Test
    void requeueReleaseBlocksOtherNodesWithinAffinityWindow() {
        TaskRef taskRef = new TaskRef("T", "4");
        node1.enqueue(taskRef);
        node1.claim(taskRef);
        node1.release(taskRef, ReleaseRequest.requeue("TRANSIENT", "boom"));

        // Fresh RELEASED row, still within the 60s affinity window — node2 must NOT win.
        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef));
    }

    @Test
    void requeueReleaseAllowsOtherNodesOnceAffinityExpires() {
        TaskRef taskRef = new TaskRef("T", "5");
        node1.enqueue(taskRef);
        node1.claim(taskRef);
        node1.release(taskRef, ReleaseRequest.requeue("TRANSIENT", "boom"));

        backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(5))); // past the 60s affinity window

        assertEquals(ClaimOutcome.WON, node2.claim(taskRef));
    }

    @Test
    void staleClaimIsStolenAfterClaimTimeout() {
        TaskRef taskRef = new TaskRef("T", "6");
        node1.enqueue(taskRef);
        node1.claim(taskRef); // simulates node-1, which then "crashes" — never releases

        backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(20))); // past the 10min claimTimeout

        assertEquals(ClaimOutcome.WON, node2.claim(taskRef));
    }

    @Test
    void freshClaimIsNotStealableBeforeClaimTimeout() {
        TaskRef taskRef = new TaskRef("T", "7");
        node1.enqueue(taskRef);
        node1.claim(taskRef);

        assertEquals(ClaimOutcome.LOST, node2.claim(taskRef));
    }

    @Test
    void exhaustionEscalatesToFailedAndDeletesHotRow() {
        TaskRef taskRef = new TaskRef("T", "8");
        node1.enqueue(taskRef);

        // maxAttempts=3 on the retry policy: 1st and 2nd requeue succeed (attempt 1, 2),
        // the 3rd attempt (attempt 3) is still <= 3 so it succeeds too; the 4th exhausts.
        for (int i = 0; i < 3; i++) {
            node1.claim(taskRef);
            assertEquals(ReleaseOutcome.OK, node1.release(taskRef, ReleaseRequest.requeue("BOOM", "fail " + i)));
            backdateUpdatedAt(taskRef, Instant.now().minus(Duration.ofMinutes(5)));
        }
        node1.claim(taskRef);
        assertEquals(ReleaseOutcome.EXHAUSTED, node1.release(taskRef, ReleaseRequest.requeue("BOOM", "final fail")));

        assertEquals(TaskStatus.FAILED, taskRow(taskRef).getStatus());
        assertTrue(inFlightRepository.findById(new TaskId("T", "8")).isEmpty(), "exhaustion is terminal — hot row must be deleted");
    }

    @Test
    void pullPendingMirrorsClaimEligibilityForTheQueryingNode() {
        TaskRef queued = new TaskRef("T", "9");
        TaskRef claimedByNode1Fresh = new TaskRef("T", "10");
        TaskRef claimedByNode1Stale = new TaskRef("T", "11");
        TaskRef releasedByNode1Fresh = new TaskRef("T", "12");
        TaskRef releasedByNode1Stale = new TaskRef("T", "13");

        for (TaskRef ref : List.of(queued, claimedByNode1Fresh, claimedByNode1Stale, releasedByNode1Fresh, releasedByNode1Stale)) {
            node1.enqueue(ref);
        }

        node1.claim(claimedByNode1Fresh);
        node1.claim(claimedByNode1Stale);
        backdateUpdatedAt(claimedByNode1Stale, Instant.now().minus(Duration.ofMinutes(20)));

        node1.claim(releasedByNode1Fresh);
        node1.release(releasedByNode1Fresh, ReleaseRequest.requeue("X", "x"));

        node1.claim(releasedByNode1Stale);
        node1.release(releasedByNode1Stale, ReleaseRequest.requeue("X", "x"));
        backdateUpdatedAt(releasedByNode1Stale, Instant.now().minus(Duration.ofMinutes(5)));

        List<TaskRef> node2Pending = node2.pullPending("T", Instant.now(), 100);

        assertTrue(node2Pending.contains(queued));
        assertTrue(node2Pending.contains(claimedByNode1Stale), "stale claim (past claimTimeout) must be stealable");
        assertTrue(node2Pending.contains(releasedByNode1Stale), "released past affinity must be open to any node");
        assertFalse(node2Pending.contains(claimedByNode1Fresh), "fresh claim by another node must not be offered");
        assertFalse(node2Pending.contains(releasedByNode1Fresh), "released within affinity must not be offered to another node");
    }

    @Test
    void enqueueIsIdempotentAndDoesNotResetAnInProgressRow() {
        TaskRef taskRef = new TaskRef("T", "14");
        node1.enqueue(taskRef);
        node1.claim(taskRef);

        node1.enqueue(taskRef); // re-enqueue while claimed — must not reset status/ownership

        TmInFlightEntity row = inFlightRow(taskRef);
        assertEquals(TaskStatus.CLAIMED, row.getStatus());
        assertEquals("node-1", row.getNodeId());
        assertEquals(TaskStatus.QUEUED, taskRow(taskRef).getStatus());
    }

    @Test
    void enqueueCreatesBothColdAndHotRowsTogether() {
        TaskRef taskRef = new TaskRef("T", "15");
        node1.enqueue(taskRef);

        assertEquals(TaskStatus.QUEUED, taskRow(taskRef).getStatus());
        Optional<TmInFlightEntity> hot = inFlightRepository.findById(new TaskId("T", "15"));
        assertTrue(hot.isPresent(), "enqueue must create the hot row eagerly, not lazily on first claim");
        assertEquals(null, hot.get().getStatus(), "unclaimed hot row starts with status=null");
    }
}
