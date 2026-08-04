package com.palmyralabs.taskmesh.max;

import com.palmyralabs.taskmesh.max.entity.PartitionedTaskId;
import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.entity.TaskStatus;
import com.palmyralabs.taskmesh.max.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.max.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency torture against a <b>real Postgres</b>, with real commits.
 *
 * <p>Only runs under {@code -Ppg}. Everything else in this module is
 * {@code @DataJpaTest}-transactional — every statement sits behind one
 * uncommitted connection that is rolled back at the end — so those suites
 * cannot exercise a cross-node race at all, no matter how many threads they
 * start. These tests opt out of the test transaction entirely and let each
 * node commit for itself, which is the only way the claim UPDATE, the D33
 * billing {@code CASE} and the D34 lock ordering are actually under test.
 *
 * <p>They also clean up after themselves, since nothing rolls back.
 */
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@EnabledIfSystemProperty(named = "spring.datasource.driver-class-name", matches = "org\\.postgresql\\.Driver")
class PostgresTortureTest {

    private static final String TYPE = "TORTURE";
    private static final String P = "P1";
    private static final int NODES = 8;
    private static final int TASKS = 300;

    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmInFlightRepository inFlightRepository;
    @Autowired private PlatformTransactionManager transactionManager;
    @jakarta.persistence.PersistenceContext private jakarta.persistence.EntityManager em;

    private TransactionTemplate tx;
    private NoopBacklogPromoter promoter;
    private List<PartitionedTaskLedger> nodes;

    @BeforeEach
    void setUp() {
        tx = new TransactionTemplate(transactionManager);
        promoter = new NoopBacklogPromoter(taskRepository, inFlightRepository, tx);
        nodes = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            nodes.add(ledgerFor("node-" + i, 3));
        }
        wipe();
    }

    @AfterEach
    void tearDown() {
        wipe();
    }

    /**
     * Bulk statements throughout, never read-modify-save: with eight nodes committing
     * for real, a row read here can be deleted by a terminal outcome before the write
     * lands, and Hibernate reports that as a {@code StaleObjectStateException} from
     * the harness rather than anything the product did.
     */
    private void wipe() {
        tx.executeWithoutResult(status -> {
            em.createQuery("DELETE FROM TmInFlightEntity f WHERE f.taskType = :t").setParameter("t", TYPE)
                    .executeUpdate();
            em.createQuery("DELETE FROM TmTaskEntity t WHERE t.taskType = :t").setParameter("t", TYPE)
                    .executeUpdate();
        });
    }

    /**
     * Reservation and affinity windows are zero so every row is contestable by every node the
     * instant it becomes claimable. Backdating rows from inside the worker threads instead — the
     * obvious alternative — turns out to deadlock the <i>harness</i>: eight concurrent
     * whole-table UPDATEs touch rows in different orders and Postgres breaks the cycle.
     */
    private PartitionedTaskLedger ledgerFor(String nodeId, int maxRetries) {
        return new PartitionedTaskLedger(taskRepository, inFlightRepository, tx, () -> nodeId,
                new ExponentialBackoffRetryPolicy(maxRetries, Duration.ofMillis(1), Duration.ofMillis(1)),
                Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ZERO,
                Duration.ofMinutes(10), Duration.ZERO);
    }

    /**
     * {@code enqueue} opens no transaction of its own either (D34) — the engine gives it one so the
     * BacklogPromoter's hot-row creation can share it. Mirrored here.
     */
    private void enqueue(TaskRef ref) {
        inTx(() -> {
            nodes.get(0).enqueue(ref);
            promoter.onEnqueue(ref, "node-0");   // this tier promotes the hot row (D31)
            return null;
        });
    }

    /** This tier requires a caller-supplied partitionKey (D32). */
    private static TaskRef partitionedRef(String taskId) {
        return new TaskRef(TYPE, taskId, P);
    }

    /** ClaimStrategy opens no transaction of its own (D34) — the caller supplies one. */
    private <T> T inTx(java.util.function.Supplier<T> work) {
        return new TransactionTemplate(transactionManager).execute(status -> work.get());
    }

    private <T> List<T> runConcurrently(int threads, java.util.function.IntFunction<T> body) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int idx = i;
                futures.add(pool.submit(() -> {
                    go.await();
                    return body.apply(idx);
                }));
            }
            go.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) {
                results.add(f.get(5, TimeUnit.MINUTES));   // a hang here is a deadlock, and must fail loudly
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    // ---- 1. the whole backlog, drained by eight nodes at once -------------

    @Test
    void aBacklogDrainedByEightNodesLosesNothingAndDuplicatesNothing() throws Exception {
        for (int i = 0; i < TASKS; i++) {
            enqueue(partitionedRef("t" + i));
        }

        Map<String, AtomicInteger> terminalsPerTask = new ConcurrentHashMap<>();
        Set<String> doubleTerminal = ConcurrentHashMap.newKeySet();

        runConcurrently(NODES, node -> {
            PartitionedTaskLedger ledger = nodes.get(node);
            int drained = 0;
            for (int round = 0; round < TASKS * 2; round++) {
                List<TaskRef> pending = inTx(() -> ledger.pullPending(TYPE, Instant.now(), 20));
                if (pending.isEmpty()) {
                    break;
                }
                for (TaskRef ref : pending) {
                    ClaimOutcome claimed = inTx(() -> ledger.claim(ref));
                    if (claimed != ClaimOutcome.WON) {
                        continue;
                    }
                    // Mixed dispositions, so retries, yields and both terminal kinds all contend.
                    int roll = Math.abs((ref.taskId() + node + round).hashCode()) % 4;
                    ReleaseRequest request = switch (roll) {
                        case 0 -> ReleaseRequest.completed();
                        case 1 -> ReleaseRequest.failedTerminal("BAD", "no good");
                        case 2 -> ReleaseRequest.retry("BOOM", "again");
                        default -> ReleaseRequest.yielded("PAUSE", "handing back");
                    };
                    ReleaseOutcome outcome = inTx(() -> ledger.release(ref, request));
                    if (isTerminal(request, outcome)
                            && terminalsPerTask.computeIfAbsent(ref.taskId(), k -> new AtomicInteger())
                                    .incrementAndGet() > 1) {
                        doubleTerminal.add(ref.taskId());
                    }
                    drained++;
                }
            }
            return drained;
        });

        assertTrue(doubleTerminal.isEmpty(),
                "a task must reach a terminal outcome at most once, but these did it twice: " + doubleTerminal);

        // Nothing may be stranded: every task is either terminal (no hot row) or still alive with one.
        long terminalCold = taskRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.COMPLETED).size()
                + taskRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.FAILED).size();
        assertEquals(terminalsPerTask.size(), terminalCold,
                "every terminal release must have left exactly one terminal cold row");

        for (TmTaskEntity cold : taskRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.COMPLETED)) {
            assertTrue(inFlightRepository.findById(new TaskId(TYPE, cold.getTaskId())).isEmpty(),
                    "a completed task must not still hold a hot row: " + cold.getTaskId());
            assertEquals(1, cold.getAttemptCount(), "no rerun happened, so every task stays on attempt 1");
            assertTrue(cold.getRetryCount() >= 0 && cold.getRetryCount() <= 3,
                    "retry_count must stay inside the budget, saw " + cold.getRetryCount()
                            + " on " + cold.getTaskId());
        }
        assertEquals(TASKS, taskRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.QUEUED).size() + terminalCold,
                "no task may be lost or duplicated across the whole run");
    }

    private static boolean isTerminal(ReleaseRequest request, ReleaseOutcome outcome) {
        if (outcome != ReleaseOutcome.OK && outcome != ReleaseOutcome.EXHAUSTED) {
            return false;
        }
        return outcome == ReleaseOutcome.EXHAUSTED
                || request.disposition() == com.palmyralabs.taskmesh.api.TerminalDisposition.COMPLETED
                || request.disposition() == com.palmyralabs.taskmesh.api.TerminalDisposition.FAILED_TERMINAL;
    }

    // ---- 2. exactly-once claim, and exactly-once billing ------------------

    @Test
    void everyContestedClaimHasOneWinnerAndBillsTheCounterExactlyOnce() throws Exception {
        TaskRef ref = partitionedRef("contested");
        enqueue(ref);

        // max_retries=3 admits ordinals 0..3, so the fifth contested round is the one that
        // terminates. Each round: eight nodes race, the winner releases RETRY (which bills
        // nothing — claim already did), and everything is aged so the next round is a fair fight.
        for (int r = 0; r <= 3; r++) {
            final int round = r;
            List<ClaimOutcome> outcomes = runConcurrently(NODES, node -> inTx(() -> nodes.get(node).claim(ref)));

            int winner = outcomes.indexOf(ClaimOutcome.WON);
            long won = outcomes.stream().filter(o -> o == ClaimOutcome.WON).count();
            long exhausted = outcomes.stream().filter(o -> o == ClaimOutcome.EXHAUSTED).count();
            assertEquals(1, won + exhausted,
                    "claim is the only cross-node gate: exactly one of " + NODES + " nodes may take it, saw "
                            + outcomes);

            assertEquals(1, won, "still within budget, so exactly one node must be admitted");
            // The winner billed exactly one unit; the seven losers moved nothing.
            assertEquals(round, inFlightRepository.findById(new TaskId(TYPE, "contested")).orElseThrow().getRetryCount(),
                    "one won claim, one unit of budget: round " + round + " must land the counter on " + round);

            ReleaseOutcome released = inTx(() -> nodes.get(winner).release(ref, ReleaseRequest.retry("BOOM", "r" + round)));
            if (round == 3) {
                // retry_count is now 3 == max_retries, so release sees there is no next execution
                // and terminates one step early rather than letting a fifth claim do it.
                assertEquals(ReleaseOutcome.EXHAUSTED, released, "the release path pre-empts the claim path");
                assertTrue(inFlightRepository.findById(new TaskId(TYPE, "contested")).isEmpty(), "hot row deleted");
                return;
            }
            assertEquals(ReleaseOutcome.OK, released, "still within budget at round " + round);
        }
    }

    // ---- 3. a task whose executions keep dying, under contention ----------

    @Test
    void aTaskWhoseNodesKeepDyingIsTerminatedExactlyOnceAndNeverOverruns() throws Exception {
        TaskRef ref = partitionedRef("dying");
        enqueue(ref);

        AtomicInteger admitted = new AtomicInteger();
        AtomicInteger terminated = new AtomicInteger();

        for (int round = 0; round < 10; round++) {
            List<ClaimOutcome> outcomes = runConcurrently(NODES, node -> inTx(() -> nodes.get(node).claim(ref)));
            outcomes.forEach(o -> {
                if (o == ClaimOutcome.WON) {
                    admitted.incrementAndGet();
                } else if (o == ClaimOutcome.EXHAUSTED) {
                    terminated.incrementAndGet();
                }
            });
            if (terminated.get() > 0) {
                break;
            }
            killLeaseAndMakeStealable(ref);   // whoever won just died without releasing
        }

        assertEquals(1, terminated.get(), "the task must be terminated exactly once, by exactly one node");
        assertEquals(4, admitted.get(),
                "max_retries=3 admits ordinals 0..3 and no more, even with eight nodes racing for every one");
        assertTrue(inFlightRepository.findById(new TaskId(TYPE, "dying")).isEmpty(), "hot row deleted");
        TmTaskEntity cold = taskRepository.findById(new PartitionedTaskId(TYPE, "dying", P)).orElseThrow();
        assertEquals(TaskStatus.FAILED, cold.getStatus());
        assertEquals("RETRY_BUDGET_EXHAUSTED", cold.getReasonCode());
        assertEquals(3, cold.getRetryCount(), "the snapshot records the last ordinal that actually ran");
    }

    // ---- 4. the lock-order inversion D34 documents ------------------------

    /**
     * D34 notes that the claim-time termination takes {@code tm_in_flight} then
     * {@code tm_task}, the reverse of {@code enqueue}/{@code release}, and that a
     * deadlock needs a stale owner releasing the very task a claim is terminating.
     * This drives exactly that shape as hard as it can and asserts Postgres never
     * has to break a cycle — a real deadlock surfaces as SQLSTATE 40P01, which
     * would be caught and counted here rather than hanging.
     */
    @Test
    void concurrentClaimTerminationAndStaleReleaseDoNotDeadlock() throws Exception {
        AtomicInteger deadlocks = new AtomicInteger();
        AtomicInteger rounds = new AtomicInteger();

        for (int i = 0; i < 40; i++) {
            TaskRef ref = partitionedRef("lockorder-" + i);
            enqueue(ref);
            // Drive it to the edge of the budget so the next claim terminates it.
            for (int r = 0; r <= 3; r++) {
                inTx(() -> nodes.get(0).claim(ref));
                killLeaseAndMakeStealable(ref);
            }

            runConcurrently(2, which -> {
                try {
                    if (which == 0) {
                        inTx(() -> nodes.get(1).claim(ref));           // holds tm_in_flight, wants tm_task
                    } else {
                        inTx(() -> nodes.get(0).release(ref,           // holds tm_task, wants tm_in_flight
                                ReleaseRequest.completed()));
                    }
                } catch (RuntimeException e) {
                    if (isDeadlock(e)) {
                        deadlocks.incrementAndGet();
                    }
                    // Anything else is a legitimate fenced-out failure, not a finding.
                }
                return null;
            });
            rounds.incrementAndGet();
        }

        assertEquals(40, rounds.get(), "all rounds must complete — a hang would have failed the future above");
        assertEquals(0, deadlocks.get(),
                "the inverted lock order must not actually deadlock in practice; "
                        + deadlocks.get() + " of 40 rounds did, which would make D34's trade-off unacceptable");
    }

    private static boolean isDeadlock(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof java.sql.SQLException sql && "40P01".equals(sql.getSQLState())) {
                return true;
            }
            if (c.getMessage() != null && c.getMessage().contains("deadlock detected")) {
                return true;
            }
        }
        return false;
    }

    // ---- helpers -------------------------------------------------------

    /** Ages every hot row past the reservation/affinity/claim-timeout windows, in one statement. */
    private void backdateAll() {
        tx.executeWithoutResult(status -> em.createQuery(
                        "UPDATE TmInFlightEntity f SET f.updatedAt = :when, f.nextRetryAt = null "
                                + "WHERE f.taskType = :t")
                .setParameter("when", Instant.now().minus(Duration.ofMinutes(30)))
                .setParameter("t", TYPE)
                .executeUpdate());
    }

    /** The owner died mid-execution: lease lapsed, never released, and now stealable by anyone. */
    private void killLeaseAndMakeStealable(TaskRef ref) {
        tx.executeWithoutResult(status -> em.createQuery(
                        "UPDATE TmInFlightEntity f SET f.leaseExpiresAt = :expired, f.updatedAt = :when, "
                                + "f.nextRetryAt = null WHERE f.taskType = :t AND f.taskId = :id")
                .setParameter("expired", Instant.now().minus(Duration.ofMinutes(1)))
                .setParameter("when", Instant.now().minus(Duration.ofMinutes(30)))
                .setParameter("t", ref.taskType())
                .setParameter("id", ref.taskId())
                .executeUpdate());
    }

    @Test
    void theHarnessItselfIsActuallyRunningAgainstPostgres() {
        String url = tx.execute(status -> System.getProperty("spring.datasource.url"));
        assertFalse(url == null || !url.startsWith("jdbc:postgresql:"),
                "these tests are meaningless on H2 — url was " + url);
    }
}
