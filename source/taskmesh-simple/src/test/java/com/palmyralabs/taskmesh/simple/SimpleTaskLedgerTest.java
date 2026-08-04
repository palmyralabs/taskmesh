package com.palmyralabs.taskmesh.simple;
import com.palmyralabs.taskmesh.simple.entity.TaskId;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.simple.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.AttemptCounters;
import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.BeforeEach;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises SimpleTaskLedger — no lease, no affinity, startup recovery
 * (infra-implementations.md Variant A), now split across the cold
 * {@code tm_task} table and hot {@code tm_in_flight} table (D-in_flight).
 */
@DataJpaTest
class SimpleTaskLedgerTest {

    @Autowired
    private TmTaskRepository taskRepository;

    @Autowired
    private TmInFlightRepository inFlightRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private SimpleTaskLedger ledger;

    @BeforeEach
    void setUp() {
        transactionTemplate = new TransactionTemplate(transactionManager);
        ledger = ledgerFor("node-1");
    }

    private SimpleTaskLedger ledgerFor(String nodeId) {
        return new SimpleTaskLedger(taskRepository, inFlightRepository, transactionTemplate, () -> nodeId,
                new ExponentialBackoffRetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1)));
    }

    private TmInFlightEntity inFlightRow(TaskRef taskRef) {
        return inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElseThrow();
    }

    private TmTaskEntity taskRow(TaskRef taskRef) {
        return taskRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElseThrow();
    }

    @Test
    void claimWinsOnceThenLosesToASecondAttempt() {
        TaskRef taskRef = new TaskRef("T", "1");
        ledger.enqueue(taskRef);

        assertEquals(ClaimOutcome.WON, ledger.claim(taskRef));

        SimpleTaskLedger other = ledgerFor("node-2");
        assertEquals(ClaimOutcome.LOST, other.claim(taskRef));
    }

    @Test
    void retryReleaseGoesStraightBackToQueuedNeverReleased() {
        TaskRef taskRef = new TaskRef("T", "2");
        ledger.enqueue(taskRef);
        ledger.claim(taskRef);

        assertEquals(ReleaseOutcome.OK, ledger.release(taskRef, ReleaseRequest.retry("BOOM", "fail")));

        assertEquals(TaskStatus.QUEUED, taskRow(taskRef).getStatus());
        TmInFlightEntity hot = inFlightRow(taskRef);
        assertNull(hot.getStatus(), "single-node has no affinity concept — hot row resets to unclaimed immediately, never RELEASED");
        assertNull(hot.getNodeId());
    }

    @Test
    void recoverOwnClaimsRequeuesThisNodesStaleClaims() {
        TaskRef taskRef = new TaskRef("T", "3");
        ledger.enqueue(taskRef);
        ledger.claim(taskRef); // simulates this node claiming, then crashing before release

        ledger.recoverOwnClaims();

        TmInFlightEntity hot = inFlightRow(taskRef);
        assertNull(hot.getStatus());
        assertNull(hot.getNodeId());
        assertEquals(ClaimOutcome.WON, ledger.claim(taskRef), "row must be claimable again after recovery");
    }

    @Test
    void recoverOwnClaimsDoesNotTouchOtherNodesClaims() {
        TaskRef taskRef = new TaskRef("T", "4");
        SimpleTaskLedger other = ledgerFor("node-2");
        other.enqueue(taskRef);
        other.claim(taskRef);

        ledger.recoverOwnClaims(); // "node-1" recovering must not touch node-2's claim

        TmInFlightEntity hot = inFlightRow(taskRef);
        assertEquals(TaskStatus.CLAIMED, hot.getStatus());
        assertEquals("node-2", hot.getNodeId());
    }

    @Test
    void pullPendingOnlyReturnsQueuedRows() {
        TaskRef queued = new TaskRef("T", "5");
        TaskRef claimed = new TaskRef("T", "6");
        ledger.enqueue(queued);
        ledger.enqueue(claimed);
        ledger.claim(claimed);

        var pending = ledger.pullPending(java.time.Instant.now(), 100);

        assertEquals(1, pending.size());
        assertEquals(queued, pending.get(0));
    }

    @Test
    void exhaustionEscalatesToFailedAndDeletesHotRow() {
        TaskRef taskRef = new TaskRef("T", "7");
        ledger.enqueue(taskRef);

        for (int i = 0; i < 3; i++) {
            ledger.claim(taskRef);
            assertEquals(ReleaseOutcome.OK, ledger.release(taskRef, ReleaseRequest.retry("BOOM", "fail " + i)));
        }
        ledger.claim(taskRef);
        assertEquals(ReleaseOutcome.EXHAUSTED, ledger.release(taskRef, ReleaseRequest.retry("BOOM", "final fail")));

        assertEquals(TaskStatus.FAILED, taskRow(taskRef).getStatus());
        assertTrue(inFlightRepository.findById(new TaskId("T", "7")).isEmpty(), "exhaustion is terminal — hot row must be deleted");
    }

    @Test
    void yieldReturnsWorkWithoutConsumingBudgetOrSettingABackoffGate() {
        TaskRef taskRef = new TaskRef("T", "8");
        ledger.enqueue(taskRef);

        // Burn one retry first, so "unchanged" is a real observation rather than 0 == 0.
        assertEquals(ClaimOutcome.WON, ledger.claim(taskRef));
        assertEquals(0, ledger.counters(taskRef).retryCount(), "the first claim bills the counter up to 0 (D33)");
        assertEquals(ReleaseOutcome.OK, ledger.release(taskRef, ReleaseRequest.retry("BOOM", "once")));
        assertEquals(0, inFlightRow(taskRef).getRetryCount(), "release does not bill — claim does");
        assertNotNull(inFlightRow(taskRef).getNextRetryAt(), "but a retry does set the backoff gate");

        assertEquals(ClaimOutcome.WON, ledger.claim(taskRef));
        assertEquals(ReleaseOutcome.OK, ledger.release(taskRef, ReleaseRequest.yielded("PAUSE_REQUESTED", "cooperative pause")));

        TmInFlightEntity hot = inFlightRow(taskRef);
        assertEquals(TaskStatus.YIELDED, hot.getStatus(), "a yield parks the row in its own status");
        assertEquals(1, hot.getAttemptCount(), "a yield is not a new attempt");
        assertEquals(1, hot.getRetryCount(), "a yield consumes no retry budget");
        assertFalse(hot.getNextRetryAt().isAfter(Instant.now()), "a yield leaves no future backoff gate — it must be immediately claimable");
        assertEquals(ClaimOutcome.WON, ledger.claim(taskRef), "immediately re-claimable after a yield");
    }

    @Test
    void rerunAfterExhaustionOpensANewAttemptWithAFullFreshRetryBudget() {
        TaskRef taskRef = new TaskRef("T", "9");
        ledger.enqueue(taskRef);

        for (int i = 0; i < 3; i++) {
            ledger.claim(taskRef);
            assertEquals(ReleaseOutcome.OK, ledger.release(taskRef, ReleaseRequest.retry("BOOM", "fail " + i)));
        }
        ledger.claim(taskRef);
        assertEquals(ReleaseOutcome.EXHAUSTED, ledger.release(taskRef, ReleaseRequest.retry("BOOM", "final fail")));
        assertEquals(1, taskRow(taskRef).getAttemptCount(), "automatic retry never moves the outer counter");
        assertEquals(3, taskRow(taskRef).getRetryCount(), "the cold snapshot records the retries actually burned");

        ledger.rerun(taskRef);

        assertEquals(new AttemptCounters(2, -1), ledger.counters(taskRef),
                "rerun opens attempt 2 with the inner counter reset to 'no execution yet' (D33)");
        for (int i = 0; i < 3; i++) {
            assertEquals(ClaimOutcome.WON, ledger.claim(taskRef));
            assertEquals(ReleaseOutcome.OK, ledger.release(taskRef, ReleaseRequest.retry("BOOM", "again " + i)),
                    "attempt 2 must get a full fresh retry budget, not re-exhaust immediately");
        }
        ledger.claim(taskRef);
        assertEquals(ReleaseOutcome.EXHAUSTED, ledger.release(taskRef, ReleaseRequest.retry("BOOM", "done")),
                "and that fresh budget is the same size, not unbounded");
    }
}
