package com.palmyralabs.taskmesh.pro;

import com.palmyralabs.taskmesh.pro.entity.TaskStatus;
import com.palmyralabs.taskmesh.pro.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.pro.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A hundred thousand tasks through a real Postgres, drained by eight nodes at
 * once. Only runs under {@code -Ppg}.
 *
 * <p>Scale is the point: the small torture suite proves the claim protocol is
 * correct, this proves it stays correct when the backlog is far larger than any
 * one poll, when {@code tm_in_flight} is big enough for the discovery index to
 * matter, and when tens of thousands of real transactions commit against each
 * other. The invariants are the ones that would quietly break at volume —
 * nothing claimed twice, nothing left stranded, nothing lost.
 */
@Tag("stress")
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@EnabledIfSystemProperty(named = "spring.datasource.driver-class-name", matches = "org\\.postgresql\\.Driver")
class PostgresLargeScaleTortureTest {

    private static final String TYPE = "TORTURE_XL";
    /** Backlog size. Override with {@code -PtortureTasks=N} for a quick diagnostic run. */
    private static final int TASKS = Integer.getInteger("taskmesh.torture.tasks", 100_000);
    /** Simulated nodes, one thread each. Override with {@code -PtortureNodes=N}. */
    private static final int NODES = Integer.getInteger("taskmesh.torture.nodes", 8);
    /**
     * Deliberately small relative to the node count. A big batch is stale by the time a node
     * reaches its tail — every other node has claimed those tasks already — so most of the batch
     * turns into lost claims, and the wasted round trips dominate.
     */
    private static final int POLL_BATCH = 400;
    /** Roughly one in ten executions fails retryably, so the retry path is under load too. */
    private static final int FAIL_ONE_IN = 10;

    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmInFlightRepository inFlightRepository;
    @Autowired private PlatformTransactionManager transactionManager;
    @jakarta.persistence.PersistenceContext private jakarta.persistence.EntityManager em;

    private TransactionTemplate tx;
    private List<MultiNodeTaskLedger> nodes;

    @BeforeEach
    void setUp() {
        tx = new TransactionTemplate(transactionManager);
        nodes = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            // Bound at construction, per D17. A lambda that computes the id when nodeId() is
            // called — `() -> "node-" + nodes.size()` — hands every ledger the *same* identity, and
            // a shared identity turns the "CLAIMED and node_id = me" eligibility branch (meant for a
            // node re-taking its own row after a restart) into a free-for-all: every thread claims
            // tasks another thread already holds, each billing a retry, and all but one lose
            // ownership at release. It reads as the engine over-failing tasks; it is not.
            String nodeId = "node-" + i;
            nodes.add(new MultiNodeTaskLedger(taskRepository, inFlightRepository, tx, () -> nodeId,
                    new ExponentialBackoffRetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1)),
                    Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ZERO,
                    Duration.ofMinutes(10), Duration.ZERO));
        }
        wipe();
    }

    @AfterEach
    void tearDown() {
        wipe();
    }

    private void wipe() {
        tx.executeWithoutResult(status -> {
            em.createQuery("DELETE FROM TmInFlightEntity f WHERE f.taskType = :t").setParameter("t", TYPE).executeUpdate();
            em.createQuery("DELETE FROM TmTaskEntity t WHERE t.taskType = :t").setParameter("t", TYPE).executeUpdate();
        });
    }

    @Test
    void aHundredThousandTasksDrainCleanlyAcrossEightNodes() throws Exception {
        // Each enqueue takes its own transaction (the ledger opens one), spread over all the nodes.
        // Wrapping thousands of them in one outer transaction instead is much slower, not faster:
        // enqueue does a findById per task, FlushModeType.AUTO flushes the pending inserts before
        // every one of those queries, and the cost grows with the persistence context — 2,000 rows
        // per transaction took minutes.
        Instant enqueueStart = Instant.now();
        runAcrossNodes(node -> {
            for (int i = node; i < TASKS; i += NODES) {
                TaskRef ref = new TaskRef(TYPE, "x" + i);
                inTx(() -> {
                    nodes.get(node).enqueue(ref);
                    return null;
                });
            }
            return null;
        });
        Duration enqueueTook = Duration.between(enqueueStart, Instant.now());
        assertEquals(TASKS, countCold(TaskStatus.QUEUED), "the whole backlog must be on disk before the drain starts");

        ConcurrentHashMap<String, Integer> terminalCount = new ConcurrentHashMap<>();
        AtomicInteger doubleTerminal = new AtomicInteger();
        AtomicLong executions = new AtomicLong();
        AtomicLong retries = new AtomicLong();
        AtomicLong claimsWon = new AtomicLong();
        AtomicLong claimsLost = new AtomicLong();
        AtomicLong claimExhausted = new AtomicLong();
        AtomicLong releaseExhausted = new AtomicLong();
        AtomicLong releaseLost = new AtomicLong();
        AtomicLong rolledFail = new AtomicLong();

        Instant drainStart = Instant.now();
        ExecutorService pool = Executors.newFixedThreadPool(NODES);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int n = 0; n < NODES; n++) {
                int idx = n;
                futures.add(pool.submit(() -> {
                    go.await();
                    MultiNodeTaskLedger ledger = nodes.get(idx);
                    int handled = 0;
                    int emptyPolls = 0;
                    while (emptyPolls < 3) {
                        // Discovery is a read, so it needs no boundary of its own — one fewer
                        // transaction per poll matters when there are 100k of them.
                        List<TaskRef> polled = ledger.pullPending(Instant.now(), POLL_BATCH);
                        // Every node's poll returns the same oldest rows in the same order, so
                        // walking them in order means all of them fight over the same task at the
                        // same moment: at batch 50 this produced 13 lost claims per task. Shuffling
                        // spreads the contention without removing it — nodes still collide, they
                        // just don't march in lockstep. Real deployments get the same effect for
                        // free from uneven processing times.
                        List<TaskRef> pending = new ArrayList<>(polled);
                        java.util.Collections.shuffle(pending, ThreadLocalRandom.current());
                        if (pending.isEmpty()) {
                            emptyPolls++;
                            Thread.sleep(25);
                            continue;
                        }
                        emptyPolls = 0;
                        for (TaskRef ref : pending) {
                            ClaimOutcome claimed = inTx(() -> ledger.claim(ref));
                            if (claimed == ClaimOutcome.EXHAUSTED) {
                                // Terminal, and it is this claim that made it so (D33) — no release
                                // follows, so it has to be counted here or the tally is short.
                                claimExhausted.incrementAndGet();
                                if (terminalCount.merge(ref.taskId(), 1, Integer::sum) > 1) {
                                    doubleTerminal.incrementAndGet();
                                }
                                continue;
                            }
                            if (claimed != ClaimOutcome.WON) {
                                claimsLost.incrementAndGet();
                                continue;   // lost the race, which is normal and silent
                            }
                            claimsWon.incrementAndGet();
                            executions.incrementAndGet();
                            // Independently random per execution. A deterministic predicate over
                            // (task hash + a per-node counter) looks like a 1-in-10 sample but is
                            // not: both terms advance in lockstep with the drain, and a failed task
                            // returns to the front of pullPending's created_at ordering, so the
                            // arithmetic re-selects the same victims. Measured on a real run, that
                            // pushed the failure rate from 10% to 69% on first execution and 99% by
                            // the third, exhausting 15% of the backlog instead of ~0.01%.
                            boolean fail = ThreadLocalRandom.current().nextInt(FAIL_ONE_IN) == 0;
                            ReleaseRequest request = fail
                                    ? ReleaseRequest.retry("BOOM", "transient at scale")
                                    : ReleaseRequest.completed();
                            ReleaseOutcome outcome = inTx(() -> ledger.release(ref, request));
                            if (fail) {
                                rolledFail.incrementAndGet();
                            }
                            if (outcome == ReleaseOutcome.LOST_OWNERSHIP) {
                                // Should be impossible here: this node holds the claim and nothing
                                // can steal it inside the claim timeout. Counted because if it ever
                                // happens, budget was billed for an execution nobody accounted for.
                                releaseLost.incrementAndGet();
                            } else if (outcome == ReleaseOutcome.OK && fail) {
                                retries.incrementAndGet();
                            } else {
                                if (outcome == ReleaseOutcome.EXHAUSTED) {
                                    releaseExhausted.incrementAndGet();
                                }
                                if (terminalCount.merge(ref.taskId(), 1, Integer::sum) > 1) {
                                    doubleTerminal.incrementAndGet();
                                }
                            }
                            handled++;
                        }
                    }
                    return handled;
                }));
            }
            go.countDown();
            for (Future<Integer> f : futures) {
                f.get(20, TimeUnit.MINUTES);
            }
        } finally {
            pool.shutdownNow();
        }
        Duration drainTook = Duration.between(drainStart, Instant.now());

        long completed = countCold(TaskStatus.COMPLETED);
        long failed = countCold(TaskStatus.FAILED);
        long stillQueued = countCold(TaskStatus.QUEUED);
        long hotLeft = inFlightRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.CLAIMED).size()
                + inFlightRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.RELEASED).size()
                + inFlightRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.YIELDED).size()
                + inFlightRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.QUEUED).size();

        System.out.printf("%n[torture-xl] %d tasks | enqueue %ds | drain %ds | %d nodes%n"
                        + "  claims: won=%d lost=%d exhausted-at-claim=%d%n"
                        + "  rolls:  failed=%d (%.1f%% of won claims)  retries-accepted=%d  exhausted-at-release=%d  lost-ownership=%d%n"
                        + "  rate:   %.0f executions/s%n",
                TASKS, enqueueTook.toSeconds(), drainTook.toSeconds(), NODES,
                claimsWon.get(), claimsLost.get(), claimExhausted.get(),
                rolledFail.get(), 100.0 * rolledFail.get() / Math.max(1, claimsWon.get()),
                retries.get(), releaseExhausted.get(), releaseLost.get(),
                executions.get() / Math.max(1.0, drainTook.toMillis() / 1000.0));

        assertEquals(0, doubleTerminal.get(), "no task may reach a terminal outcome twice");
        assertEquals(0, stillQueued, "the backlog must be fully drained");
        assertEquals(0, hotLeft, "a terminal outcome deletes the hot row — none may be stranded");
        assertEquals(TASKS, completed + failed, "every task must end terminal exactly once, and none may be lost");
        assertEquals(TASKS, terminalCount.size(), "…and the drain loop must have seen each of them terminate");
        assertTrue(retries.get() > 0, "the retry path must actually have been exercised at this scale");
        assertTrue(executions.get() >= TASKS + retries.get(),
                "every task ran at least once, plus one extra execution per retry");

        // With an independent 1-in-10 roll and a budget of four executions, exhausting takes four
        // consecutive failures: ~0.01% of the backlog. Anything wildly above that means the rolls
        // have stopped being independent — which is exactly how the first version of this test was
        // wrong, and it would otherwise masquerade as the engine over-failing tasks.
        assertTrue(failed < TASKS / 100,
                "far more exhaustions than four independent 1-in-10 failures can explain: " + failed
                        + " of " + TASKS + " — suspect the harness, not the ledger");
        for (var row : taskRepository.findByTaskTypeAndStatus(TYPE, TaskStatus.FAILED)) {
            assertEquals(3, row.getRetryCount(),
                    "an exhausted task burned exactly max_retries retries, no more and no fewer");
        }
    }

    /** Runs one job per simulated node, and fails loudly if any of them hangs. */
    private <T> List<T> runAcrossNodes(java.util.function.IntFunction<T> body) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(NODES);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (int n = 0; n < NODES; n++) {
                int idx = n;
                futures.add(pool.submit(() -> {
                    go.await();
                    return body.apply(idx);
                }));
            }
            go.countDown();
            List<T> out = new ArrayList<>();
            for (Future<T> f : futures) {
                out.add(f.get(20, TimeUnit.MINUTES));
            }
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    private long countCold(TaskStatus status) {
        return taskRepository.findByTaskTypeAndStatus(TYPE, status).size();
    }

    private <T> T inTx(java.util.function.Supplier<T> work) {
        return new TransactionTemplate(transactionManager).execute(status -> work.get());
    }
}
