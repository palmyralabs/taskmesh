package com.palmyralabs.taskmesh.max;
import com.palmyralabs.taskmesh.max.entity.PartitionedTaskId;
import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        promoter = windowed(500, 1000);   // production defaults; far above anything these tests enqueue
    }

    /** A promoter with an explicit in-flight window, for the watermark tests below. */
    private JpaBacklogPromoter windowed(int low, int high) {
        return windowed(ledger, low, high);
    }

    private JpaBacklogPromoter windowed(PartitionedTaskLedger against, int low, int high) {
        return new JpaBacklogPromoter(taskRepository, inFlightRepository, transactionTemplate, against, "node-1", low, high);
    }

    private void enqueueAll(String taskType, int count) {
        for (int i = 0; i < count; i++) {
            ledger.enqueue(new TaskRef(taskType, taskType + "-" + i, P));
        }
    }

    /** Total hot rows — the table level, deliberately not the claimable level the window regulates. */
    private long hotRows() {
        return inFlightRepository.count();
    }

    /** What the poller would actually pick up: the level the window is measured against. */
    private long claimable() {
        return ledger.countClaimable(Instant.now());
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

    // ---- in-flight window -------------------------------------------------------

    /**
     * The whole point of the window: a backlog far larger than the high watermark
     * must not land in {@code tm_in_flight} in one go, however generous the engine's
     * per-cycle limit is.
     */
    @Test
    void promoteStopsAtTheHighWatermarkNoMatterHowDeepTheBacklogIs() {
        enqueueAll("W1", 40);

        int promoted = windowed(3, 5).promote("W1", Instant.now(), 1000);

        assertEquals(5, promoted, "promotion fills the window, not the backlog");
        assertEquals(5, hotRows(), "and tm_in_flight is held at the high watermark");
    }

    /**
     * Hysteresis. Between the two marks the promoter does nothing at all — otherwise
     * a busy queue would be topped up by a row or two on every cycle, paying a
     * transaction each time for almost no work.
     */
    @Test
    void promoteDoesNothingWhileTheLevelIsStillAboveTheLowWatermark() {
        enqueueAll("W2", 40);
        JpaBacklogPromoter promoter = windowed(3, 5);
        assertEquals(5, promoter.promote("W2", Instant.now(), 1000));

        assertEquals(0, promoter.promote("W2", Instant.now(), 1000),
                "level is 5, still at/above the low watermark of 3 — nothing to do");
        assertEquals(0, promoter.promote("W2", Instant.now(), 1000), "and it stays that way across cycles");
        assertEquals(5, hotRows());
    }

    /**
     * ...and it does resume. This is the regulation half: a rate-only throttle would
     * have kept promoting regardless of the level, and a level check with no refill
     * path would stall the queue permanently once it dipped.
     */
    @Test
    void promoteTopsBackUpOnceTheLevelFallsBelowTheLowWatermark() {
        enqueueAll("W3", 40);
        JpaBacklogPromoter promoter = windowed(3, 5);
        promoter.promote("W3", Instant.now(), 1000);
        assertEquals(5, hotRows());

        // Three tasks reach a terminal outcome, which deletes their hot rows: level 5 -> 2.
        transactionTemplate.executeWithoutResult(status -> {
            for (int i = 0; i < 3; i++) {
                inFlightRepository.deleteByTaskTypeAndTaskId("W3", "W3-" + i);
            }
        });
        assertEquals(2, hotRows(), "precondition: the level dipped below the low watermark");

        assertEquals(3, promoter.promote("W3", Instant.now(), 1000), "refills the deficit, 2 -> 5");
        assertEquals(5, hotRows(), "back to the high watermark");
    }

    /**
     * The engine's {@code limit} still caps one cycle — the {@code BacklogPromoter}
     * contract is "up to limit", and a single promotion transaction stays bounded
     * however deep the deficit is. The window is then reached over several cycles.
     */
    @Test
    void promoteHonoursTheEngineLimitWhenItIsSmallerThanTheDeficit() {
        enqueueAll("W4", 40);
        JpaBacklogPromoter promoter = windowed(10, 20);

        assertEquals(4, promoter.promote("W4", Instant.now(), 4), "capped by limit, not by the deficit of 20");
        assertEquals(4, promoter.promote("W4", Instant.now(), 4), "level 4 is still below the low watermark of 10");
        assertEquals(8, hotRows());
    }

    /**
     * A misconfigured window fails at construction rather than presenting later as a
     * queue that silently never drains — a zero low mark promotes nothing ever, and an
     * inverted pair computes a negative deficit.
     */
    @Test
    void rejectsAWindowThatCouldNeverPromoteAnything() {
        assertThrows(IllegalArgumentException.class, () -> windowed(0, 100), "a zero low watermark never promotes");
        assertThrows(IllegalArgumentException.class, () -> windowed(-1, 100), "nor a negative one");
        assertThrows(IllegalArgumentException.class, () -> windowed(100, 99), "nor an inverted window");
    }

    /** A single mark (low == high) is a legitimate no-hysteresis configuration, not a mistake. */
    @Test
    void acceptsAWindowWithBothMarksEqual() {
        enqueueAll("W5", 10);

        assertEquals(4, windowed(4, 4).promote("W5", Instant.now(), 1000));
        assertEquals(4, hotRows());
    }

    /**
     * The reason the window is measured with {@code countClaimable} and not a plain
     * {@code COUNT(*)}: a row released for retry keeps its hot row but is unclaimable until
     * {@code next_retry_at} passes. Counting rows rather than claimable work would read that
     * as a full window and stop promoting — starving every worker precisely when the failure
     * rate is highest, which is when the queue can least afford it.
     */
    @Test
    void rowsInsideTheirRetryBackoffDoNotCountTowardsTheWindow() {
        // Retries back off for minutes here, so a released row is genuinely unclaimable for
        // the whole test rather than for the 1ms the other tests' ledger uses.
        PartitionedTaskLedger backingOff = new PartitionedTaskLedger(taskRepository, inFlightRepository,
                transactionTemplate, () -> "node-1",
                new ExponentialBackoffRetryPolicy(3, Duration.ofMinutes(10), Duration.ofMinutes(10)),
                Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ofSeconds(60),
                Duration.ofMinutes(10), Duration.ofSeconds(60));
        JpaBacklogPromoter promoter = windowed(backingOff, 2, 2);

        enqueueAll("B", 10);
        assertEquals(2, promoter.promote("B", Instant.now(), 1000));
        assertEquals(2, backingOff.countClaimable(Instant.now()), "window filled");

        // Both promoted tasks fail retryably — within budget, so they keep their hot rows.
        transactionTemplate.executeWithoutResult(status -> {
            for (int i = 0; i < 2; i++) {
                TaskRef ref = new TaskRef("B", "B-" + i, P);
                ledgerClaimAndRetry(backingOff, ref);
            }
        });

        assertEquals(2, hotRows(), "a retry keeps its hot row — the rows are still on the table");
        assertEquals(0, backingOff.countClaimable(Instant.now()),
                "but none is claimable: next_retry_at is ten minutes out");

        assertEquals(2, promoter.promote("B", Instant.now(), 1000),
                "so as far as the poller is concerned the window is empty, and promotion refills it");
        assertEquals(4, hotRows(),
                "and tm_in_flight now exceeds the window — backed-off retries sit on top of it, by design");
    }

    private void ledgerClaimAndRetry(PartitionedTaskLedger against, TaskRef ref) {
        against.claim(ref);
        against.release(ref, ReleaseRequest.retry("BOOM", "transient failure"));
    }
}
