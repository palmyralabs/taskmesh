package com.palmyralabs.taskmesh.simple;

import com.palmyralabs.taskmesh.simple.entity.TaskId;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.simple.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;

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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the release-side terminal contract, both halves and both negatives:
 *
 * <ul>
 *   <li><b>success</b> → delete the hot row, update the cold row to
 *       {@code COMPLETED};</li>
 *   <li><b>retryable failure with the budget exhausted</b> → delete the hot
 *       row, update the cold row to {@code FAILED};</li>
 *   <li><b>retryable failure within budget</b> → touch <i>neither</i>: the hot
 *       row survives and the cold row stays {@code QUEUED};</li>
 *   <li><b>lost ownership</b> → touch <i>neither</i>, whatever the
 *       disposition.</li>
 * </ul>
 *
 * <p>The negatives are the point. "Deletes the hot row on a terminal outcome"
 * is only a real guarantee if a non-terminal outcome demonstrably doesn't, and
 * an {@code isEmpty()} assertion proves nothing unless the row was there to
 * begin with — so every case here checks the before-state too. D33 moved
 * retry billing to {@code claim()}; none of this was supposed to change, and
 * this test is what says so.
 */
@DataJpaTest
class TerminalReleaseContractTest {

    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmInFlightRepository inFlightRepository;
    @Autowired private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private MultiNodeTaskLedger node1;
    private MultiNodeTaskLedger node2;
    private SimpleTaskLedger singleNode;

    @BeforeEach
    void setUp() {
        transactionTemplate = new TransactionTemplate(transactionManager);
        node1 = multiNode("node-1");
        node2 = multiNode("node-2");
        singleNode = new SimpleTaskLedger(taskRepository, inFlightRepository, transactionTemplate, () -> "solo",
                new ExponentialBackoffRetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1)));
    }

    private MultiNodeTaskLedger multiNode(String nodeId) {
        return new MultiNodeTaskLedger(taskRepository, inFlightRepository, transactionTemplate, () -> nodeId,
                new ExponentialBackoffRetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1)),
                Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ofSeconds(60),
                Duration.ofMinutes(10), Duration.ofSeconds(60));
    }

    private Optional<TmInFlightEntity> hot(TaskRef ref) {
        return inFlightRepository.findById(new TaskId(ref.taskType(), ref.taskId()));
    }

    private TmTaskEntity cold(TaskRef ref) {
        return taskRepository.findById(new TaskId(ref.taskType(), ref.taskId())).orElseThrow();
    }

    // ---- success -> delete hot, cold = COMPLETED ------------------------

    @Test
    void aSuccessfulReleaseDeletesTheHotRowAndCompletesTheColdRow() {
        TaskRef ref = new TaskRef("REL", "ok-1");
        node1.enqueue(ref);
        assertEquals(ClaimOutcome.WON, node1.claim(ref));
        assertTrue(hot(ref).isPresent(), "precondition: the hot row exists while the task is claimed");
        assertEquals(TaskStatus.QUEUED, cold(ref).getStatus(), "precondition: the cold row is not terminal yet");

        assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.completed()));

        assertTrue(hot(ref).isEmpty(), "success is terminal — the hot row must be deleted");
        TmTaskEntity coldRow = cold(ref);
        assertEquals(TaskStatus.COMPLETED, coldRow.getStatus(), "and the cold row must record COMPLETED");
        assertEquals(1, coldRow.getAttemptCount(), "with the counters snapshotted off the hot row before it went");
        assertEquals(0, coldRow.getRetryCount(), "it succeeded on its first execution");
        assertNull(coldRow.getReasonCode(), "a success carries no reason code");
        assertNull(coldRow.getErrorSummary());
        assertNotNull(coldRow.getUpdatedAt());
        assertEquals(ClaimOutcome.LOST, node2.claim(ref), "and nothing can claim it again");
    }

    @Test
    void aSuccessAfterRetriesSnapshotsTheRetriesItBurned() {
        TaskRef ref = new TaskRef("REL", "ok-2");
        node1.enqueue(ref);
        for (int i = 0; i < 2; i++) {
            node1.claim(ref);
            assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.retry("BOOM", "fail " + i)));
        }
        node1.claim(ref);

        assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.completed()));

        assertTrue(hot(ref).isEmpty());
        assertEquals(TaskStatus.COMPLETED, cold(ref).getStatus());
        assertEquals(1, cold(ref).getAttemptCount(), "no operator rerun, so still attempt 1");
        assertEquals(2, cold(ref).getRetryCount(), "two retries were burned before it succeeded");
    }

    // ---- retryable failure, budget exhausted -> delete hot, cold = FAILED ----

    @Test
    void aRetryableFailureThatExhaustsTheBudgetDeletesTheHotRowAndFailsTheColdRow() {
        TaskRef ref = new TaskRef("REL", "dead-1");
        node1.enqueue(ref);

        // max_retries=3 admits ordinals 0..3; the fourth execution's failure has nowhere to go.
        for (int i = 0; i < 3; i++) {
            node1.claim(ref);
            assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.retry("BOOM", "fail " + i)),
                    "still within budget");
        }
        node1.claim(ref);
        assertTrue(hot(ref).isPresent(), "precondition: the hot row is still there going into the last release");
        assertEquals(TaskStatus.QUEUED, cold(ref).getStatus(), "precondition: the cold row is still non-terminal");

        assertEquals(ReleaseOutcome.EXHAUSTED, node1.release(ref, ReleaseRequest.retry("LAST", "gave up")),
                "the caller asked for a retry and must be told it became terminal instead (D19)");

        assertTrue(hot(ref).isEmpty(), "exhaustion is terminal — the hot row must be deleted");
        TmTaskEntity coldRow = cold(ref);
        assertEquals(TaskStatus.FAILED, coldRow.getStatus(), "and the cold row must record FAILED");
        assertEquals("LAST", coldRow.getReasonCode(), "carrying the failure's own reason code");
        assertEquals("gave up", coldRow.getErrorSummary());
        assertEquals(1, coldRow.getAttemptCount());
        assertEquals(3, coldRow.getRetryCount(), "and the retries it actually burned");
        assertEquals(ClaimOutcome.LOST, node2.claim(ref), "nothing can claim it again");
    }

    @Test
    void aNonRetryableFailureDeletesTheHotRowAndFailsTheColdRowImmediately() {
        TaskRef ref = new TaskRef("REL", "dead-2");
        node1.enqueue(ref);
        node1.claim(ref);
        assertTrue(hot(ref).isPresent());

        assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.failedTerminal("BAD_INPUT", "unparseable")));

        assertTrue(hot(ref).isEmpty(), "a non-retryable failure is terminal on its first execution");
        assertEquals(TaskStatus.FAILED, cold(ref).getStatus());
        assertEquals("BAD_INPUT", cold(ref).getReasonCode());
        assertEquals("unparseable", cold(ref).getErrorSummary());
        assertEquals(0, cold(ref).getRetryCount(), "it never got to retry");
    }

    // ---- the negatives -------------------------------------------------

    @Test
    void aRetryableFailureWithinBudgetTouchesNeitherRow() {
        TaskRef ref = new TaskRef("REL", "alive-1");
        node1.enqueue(ref);
        node1.claim(ref);

        assertEquals(ReleaseOutcome.OK, node1.release(ref, ReleaseRequest.retry("BOOM", "try again")));

        TmInFlightEntity hotRow = hot(ref).orElseThrow(() ->
                new AssertionError("a retry within budget must NOT delete the hot row — the task is still alive"));
        assertEquals(TaskStatus.RELEASED, hotRow.getStatus(), "it is handed back, not finished");
        assertNotNull(hotRow.getNextRetryAt(), "with a backoff gate for the next execution");

        TmTaskEntity coldRow = cold(ref);
        assertEquals(TaskStatus.QUEUED, coldRow.getStatus(),
                "and the cold row must NOT be written — it is only ever touched at enqueue and at a terminal outcome");
        assertNull(coldRow.getReasonCode(), "in particular the failure's reason code must not leak onto it yet");
    }

    @Test
    void aReleaseThatLostOwnershipTouchesNeitherRowForAnyDisposition() {
        TaskRef ref = new TaskRef("REL", "stolen-1");
        node1.enqueue(ref);
        node1.claim(ref);

        // node-2 steals it: lease lapsed and the row went quiet past the claim timeout.
        transactionTemplate.executeWithoutResult(status -> {
            TmInFlightEntity row = hot(ref).orElseThrow();
            row.setLeaseExpiresAt(Instant.now().minus(Duration.ofMinutes(1)));
            row.setUpdatedAt(Instant.now().minus(Duration.ofMinutes(30)));
            inFlightRepository.save(row);
        });
        assertEquals(ClaimOutcome.WON, node2.claim(ref));

        // Everything node-1 tries from here is unfenced and must write nothing at all.
        assertEquals(ReleaseOutcome.LOST_OWNERSHIP, node1.release(ref, ReleaseRequest.completed()));
        assertEquals(ReleaseOutcome.LOST_OWNERSHIP, node1.release(ref, ReleaseRequest.failedTerminal("X", "x")));
        assertEquals(ReleaseOutcome.LOST_OWNERSHIP, node1.release(ref, ReleaseRequest.retry("X", "x")));

        assertTrue(hot(ref).isPresent(), "the hot row belongs to node-2 now and must survive node-1's releases");
        assertEquals("node-2", hot(ref).orElseThrow().getNodeId());
        assertEquals(TaskStatus.CLAIMED, hot(ref).orElseThrow().getStatus());
        assertEquals(TaskStatus.QUEUED, cold(ref).getStatus(), "and the cold row must never be written by a loser");
        assertNull(cold(ref).getReasonCode());
    }

    // ---- single-node tier keeps the same contract ------------------------

    @Test
    void theSingleNodeTierHonoursBothTerminalPathsToo() {
        TaskRef ok = new TaskRef("REL", "solo-ok");
        singleNode.enqueue(ok);
        singleNode.claim(ok);
        assertTrue(hot(ok).isPresent());
        assertEquals(ReleaseOutcome.OK, singleNode.release(ok, ReleaseRequest.completed()));
        assertTrue(hot(ok).isEmpty(), "success deletes the hot row here as well");
        assertEquals(TaskStatus.COMPLETED, cold(ok).getStatus());

        TaskRef dead = new TaskRef("REL", "solo-dead");
        singleNode.enqueue(dead);
        for (int i = 0; i < 3; i++) {
            singleNode.claim(dead);
            assertEquals(ReleaseOutcome.OK, singleNode.release(dead, ReleaseRequest.retry("BOOM", "fail " + i)));
            assertTrue(hot(dead).isPresent(), "still alive while within budget");
        }
        singleNode.claim(dead);
        assertEquals(ReleaseOutcome.EXHAUSTED, singleNode.release(dead, ReleaseRequest.retry("LAST", "gave up")));
        assertTrue(hot(dead).isEmpty(), "exhaustion deletes the hot row here as well");
        assertEquals(TaskStatus.FAILED, cold(dead).getStatus());
        assertEquals("LAST", cold(dead).getReasonCode());
        assertEquals(3, cold(dead).getRetryCount());
    }
}
