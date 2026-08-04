package com.palmyralabs.taskmesh.max;
import com.palmyralabs.taskmesh.max.entity.PartitionedTaskId;
import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the throttled promotion path: onEnqueue is a no-op, promote() is bounded and idempotent per row. */
@DataJpaTest
class JpaBacklogPromoterTest {

    private static final String P = "P1";

    @Autowired
    private TmTaskRepository taskRepository;

    @Autowired
    private TmInFlightRepository inFlightRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private PartitionedTaskLedger ledger;
    private JpaBacklogPromoter promoter;

    @BeforeEach
    void setUp() {
        transactionTemplate = new TransactionTemplate(transactionManager);
        ledger = new PartitionedTaskLedger(taskRepository, inFlightRepository, transactionTemplate, () -> "node-1",
                new ExponentialBackoffRetryPolicy(3, java.time.Duration.ofMillis(1), java.time.Duration.ofMillis(1)),
                java.time.Duration.ofMinutes(5), java.time.Duration.ofSeconds(30), java.time.Duration.ofSeconds(60),
                java.time.Duration.ofMinutes(10), java.time.Duration.ofSeconds(60));
        promoter = new JpaBacklogPromoter(taskRepository, inFlightRepository, transactionTemplate, "node-1");
    }

    @Test
    void onEnqueueIsANoOpAndLeavesNoHotRow() {
        TaskRef taskRef = new TaskRef("T", "1", P);
        ledger.enqueue(taskRef);
        promoter.onEnqueue(taskRef, "node-1");

        assertTrue(inFlightRepository.findById(new TaskId("T", "1")).isEmpty(),
                "throttled promoter must never create the hot row from onEnqueue");
    }

    @Test
    void promoteRespectsLimitAndOnlyPicksUpUnpromotedRows() {
        for (int i = 0; i < 5; i++) {
            ledger.enqueue(new TaskRef("T", String.valueOf(i), P));
        }

        int promoted = promoter.promote("T", Instant.now(), 3);

        assertEquals(3, promoted);
        long hotCount = 0;
        for (int i = 0; i < 5; i++) {
            if (inFlightRepository.findById(new TaskId("T", String.valueOf(i))).isPresent()) {
                hotCount++;
            }
        }
        assertEquals(3, hotCount, "only the batch limit's worth of hot rows should exist yet");
    }

    @Test
    void promoteStampsPromotedAtSoARepeatCallDoesNotRePromote() {
        TaskRef taskRef = new TaskRef("T", "1", P);
        ledger.enqueue(taskRef);

        assertEquals(1, promoter.promote("T", Instant.now(), 10));
        assertEquals(0, promoter.promote("T", Instant.now(), 10), "already-promoted row must not be picked up again");

        assertTrue(taskRepository.findById(new PartitionedTaskId("T", "1", P)).orElseThrow().getPromotedAt() != null);
    }

    @Test
    void promoteReturnsZeroWhenBacklogIsEmpty() {
        assertEquals(0, promoter.promote("T", Instant.now(), 10));
    }

    @Test
    void promoteCarriesPartitionKeyOntoTheCreatedHotRow() {
        TaskRef taskRef = new TaskRef("T", "1", P);
        ledger.enqueue(taskRef);

        promoter.promote("T", Instant.now(), 10);

        assertEquals(P, inFlightRepository.findById(new TaskId("T", "1")).orElseThrow().getPartitionKey(),
                "promoted hot row must carry the same partitionKey as its source tm_task row");
    }

    /**
     * The hazard {@code NoopBacklogPromoter}'s javadoc spells out and this promoter used to be
     * missing: {@code save()} on a manually-assigned composite key is an upsert, so promoting a
     * task that already has a hot row silently resets a claimed task to unclaimed and loses the
     * owner's claim. "Unpromoted" normally implies "no hot row", but it is one missed
     * {@code promoted_at} stamp away from not implying it — and the failure is silent.
     */
    @Test
    void promoteMustNotUpsertOverAnExistingHotRow() {
        TaskRef taskRef = new TaskRef("T", "guard-1", P);
        ledger.enqueue(taskRef);
        assertEquals(com.palmyralabs.taskmesh.api.ClaimOutcome.WON, transactionTemplate.execute(st -> {
            promoter.promote("T", Instant.now(), 10);       // creates the hot row, stamps promoted_at
            return ledger.claim(taskRef);                    // and a node takes it
        }));

        // Force the shape the guard exists for: a live, claimed hot row whose cold row still looks
        // unpromoted. Without the guard the next cycle upserts a fresh QUEUED row over the claim.
        transactionTemplate.executeWithoutResult(st -> taskRepository
                .findById(new PartitionedTaskId("T", "guard-1", P))
                .ifPresent(task -> {
                    task.setPromotedAt(null);
                    taskRepository.save(task);
                }));

        promoter.promote("T", Instant.now(), 10);

        var hot = inFlightRepository.findById(new TaskId("T", "guard-1")).orElseThrow();
        assertEquals(com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED, hot.getStatus(),
                "promotion must leave a claimed task alone, not reset it to unclaimed");
        assertEquals("node-1", hot.getNodeId(), "and must not take the claim away from its owner");
    }

    /**
     * {@code rerun} creates the hot row itself rather than leaving it to this promoter, so it has
     * to stamp {@code promoted_at} too — otherwise a rerun of a task still sitting in the backlog
     * leaves exactly the unpromoted-with-a-live-hot-row shape the guard above has to catch.
     */
    @Test
    void rerunStampsPromotedAtSoTheBacklogScanSkipsTheTaskItJustRevived() {
        TaskRef taskRef = new TaskRef("T", "rerun-1", P);
        ledger.enqueue(taskRef);   // enqueued but never promoted — still backlogged

        transactionTemplate.executeWithoutResult(st -> ledger.rerun(taskRef));

        var cold = taskRepository.findById(new PartitionedTaskId("T", "rerun-1", P)).orElseThrow();
        assertTrue(cold.getPromotedAt() != null,
                "rerun created the hot row, so the cold row is promoted and must say so");
        assertTrue(inFlightRepository.findById(new TaskId("T", "rerun-1")).isPresent(), "and the hot row exists");

        assertEquals(0, promoter.promote("T", Instant.now(), 10),
                "so the backlog scan must not pick it up at all");
    }
}
