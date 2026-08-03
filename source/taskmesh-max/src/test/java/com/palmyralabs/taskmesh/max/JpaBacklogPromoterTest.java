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
}
