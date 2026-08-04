package com.palmyralabs.taskmesh.max;

import com.palmyralabs.taskmesh.api.MetricsReporter;
import com.palmyralabs.taskmesh.api.PauseController;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.api.TerminalContext;
import com.palmyralabs.taskmesh.api.TerminalOutcome;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.exception.NonRetryableException;
import com.palmyralabs.taskmesh.max.entity.TaskStatus;
import com.palmyralabs.taskmesh.max.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.max.repository.TmAuditEventRepository;
import com.palmyralabs.taskmesh.max.repository.TmControlFlagRepository;
import com.palmyralabs.taskmesh.max.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;
import com.palmyralabs.taskmesh.max.starter.TaskmeshProfileBuilder;
import com.palmyralabs.taskmesh.starter.BacklogPromotion;
import com.palmyralabs.taskmesh.starter.TaskmeshProperties;
import com.palmyralabs.taskmesh.starter.WorkloadProfile;

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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end throughput through the <b>real engine</b> against a <b>real
 * Postgres</b>. Only runs under {@code -Ppg}.
 *
 * <p>Complements the torture suites rather than repeating them. Those drive
 * {@link PartitionedTaskLedger} directly with a coin flip standing in for work,
 * which is the right shape for proving the claim protocol but means the engine
 * is never in the picture. This boots {@code TaskMeshEngineImpl} on every
 * simulated node — poller, execution queue, worker pool, dispatcher, heartbeat,
 * history writer, incident store — and puts real {@link TaskProcessor}s behind
 * it, so what gets measured is what a consuming application would actually see.
 *
 * <p>Phases are separated on purpose. All tasks are enqueued through
 * {@link TaskMeshEngine#enqueue} while the cluster is held paused, so nothing is
 * claimed until the backlog is fully on disk; {@code attempt()} checks the pause
 * flag before claiming and covers the enqueue fast-path as well as the poller,
 * so the boundary is the engine's own documented behaviour, not a test hack.
 * Un-pausing starts the drain, and the drain clock runs until the last
 * {@code tm_in_flight} row is gone.
 *
 * <p>{@code showSql = false} matters here: {@code @DataJpaTest} defaults it to
 * true, and at this volume the resulting stdout dominates both the wall clock
 * and the test-results XML (the 100k torture runs produce ~850MB of it). These
 * numbers are meant to be about the engine.
 */
@Tag("stress")
@DataJpaTest(showSql = false)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@EnabledIfSystemProperty(named = "spring.datasource.driver-class-name", matches = "org\\.postgresql\\.Driver")
class PostgresEngineThroughputTest {

    private static final String TYPE_OK = "ENGINE_OK";
    private static final String TYPE_FAIL = "ENGINE_FAIL";
    private static final List<String> TYPES = List.of(TYPE_OK, TYPE_FAIL);

    /** Backlog of ordinary tasks. Override with {@code -PengineTasks=N}. */
    private static final int TASKS = Integer.getInteger("taskmesh.engine.tasks", 10_000);
    /** Engines, i.e. simulated nodes — each a full TaskMeshEngineImpl. Override with {@code -PengineNodes=N}. */
    private static final int NODES = Integer.getInteger("taskmesh.engine.nodes", 3);
    /** Worker threads per engine. Override with {@code -PengineWorkers=N}. */
    private static final int WORKERS = Integer.getInteger("taskmesh.engine.workers", 8);
    /** Poller batch size per cycle (capped by the execution queue's remaining capacity). */
    private static final int BATCH = Integer.getInteger("taskmesh.engine.batch", 100);
    /** Execution-queue depth, including the fixed 5-slot priority lane. {@code -PengineQueue=}. */
    private static final int QUEUE = Integer.getInteger("taskmesh.engine.queue", 100);
    /**
     * Poll interval in seconds. With adaptive polling this is the *ceiling* on the cadence, and
     * PollCadence floors it at 1s — so leaving it at 1 pins the window shut and the cadence can
     * never move. {@code -PengineInterval=}.
     */
    private static final int POLL_INTERVAL_SECONDS = Integer.getInteger("taskmesh.engine.interval", 1);
    /** SHA-256 rounds per execution — stands in for real work. Override with {@code -PengineWork=N}. */
    private static final int WORK_ROUNDS = Integer.getInteger("taskmesh.engine.work", 200);
    /**
     * Simulated blocking work per execution: uniform random 0..N milliseconds ({@code -PengineSleep=}). 0 = none.
     * The SHA-256 rounds above cost ~13us, so without this the measurement is almost entirely
     * engine overhead; a real processor that talks to anything spends orders of magnitude longer,
     * which changes which resource is scarce.
     */
    private static final long SLEEP_MILLIS = Long.getLong("taskmesh.engine.sleep", 0L);
    /** Distinct partition_key buckets, so the run is not accidentally single-partition (D32). */
    private static final int PARTITIONS = 8;

    /** {@code EAGER} (hot row at enqueue) or {@code THROTTLED} (in-flight window). {@code -PenginePromotion=}. */
    private static final BacklogPromotion PROMOTION =
            BacklogPromotion.valueOf(System.getProperty("taskmesh.engine.promotion", "EAGER"));
    private static final int LOW_WATERMARK = Integer.getInteger("taskmesh.engine.lowWatermark", 500);
    private static final int HIGH_WATERMARK = Integer.getInteger("taskmesh.engine.highWatermark", 1000);
    /**
     * Promotion cadence. The defaults (500 rows / 5s) cap refill at 100 rows/s, which is well under
     * what these nodes drain — so a comparison run has to size them against the drain rate or it
     * measures the promoter's schedule rather than the engine.
     */
    private static final int BACKLOG_BATCH = Integer.getInteger("taskmesh.engine.backlogBatch", 500);
    private static final int BACKLOG_INTERVAL = Integer.getInteger("taskmesh.engine.backlogInterval", 1);

    /** Roughly one execution in this many throws retryably, so the retry path is under load. */
    private static final int FAIL_ONE_IN = 20;
    /** Tasks of a type whose processor always throws non-retryably — drives terminal FAILED + incident. */
    private static final int FAIL_TASKS = Math.max(1, TASKS / 50);
    private static final int MAX_RETRIES = 3;

    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmInFlightRepository inFlightRepository;
    @Autowired private TmHistoryEventRepository historyEventRepository;
    @Autowired private TmAttemptHistoryRepository attemptHistoryRepository;
    @Autowired private TmAuditEventRepository auditEventRepository;
    @Autowired private TmIncidentRepository incidentRepository;
    @Autowired private TmControlFlagRepository controlFlagRepository;
    @Autowired private PlatformTransactionManager transactionManager;
    @jakarta.persistence.PersistenceContext private jakarta.persistence.EntityManager em;

    private TransactionTemplate tx;
    private TogglePauseController pause;
    private MeasuredProcessor okProcessor;
    private MeasuredProcessor failProcessor;
    private CountingMetricsReporter metrics;
    private List<TaskMeshEngine> engines;
    /** High-water mark of tm_in_flight across the drain — EAGER holds the whole backlog, THROTTLED a window. */
    private long peakInFlight;

    @BeforeEach
    void setUp() {
        tx = new TransactionTemplate(transactionManager);
        wipe();
        pause = new TogglePauseController(true);   // held paused: enqueue must not race the drain
        okProcessor = new MeasuredProcessor(TYPE_OK, WORK_ROUNDS, FAIL_ONE_IN, false);
        failProcessor = new MeasuredProcessor(TYPE_FAIL, WORK_ROUNDS, 0, true);
        metrics = new CountingMetricsReporter();
        engines = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            engines.add(engineFor("engine-node-" + i));
        }
    }

    @AfterEach
    void tearDown() {
        if (engines != null) {
            engines.forEach(TaskMeshEngine::stop);
        }
        wipe();
    }

    /**
     * One engine per simulated node, wired exactly the way the tier's own
     * auto-configuration wires it — {@link TaskmeshProfileBuilder} shares
     * {@code ProfileFactories} with {@code TaskmeshEngineAutoConfiguration}, so
     * this is the production mapping, not a test-local approximation. Only the
     * pause controller, retry policy and metrics reporter are overridden, and
     * each of those is a documented override point. The backlog promoter is left
     * at this tier's default {@code NoopBacklogPromoter}, so hot rows are created
     * eagerly at enqueue rather than throttled (D31).
     */
    private TaskMeshEngine engineFor(String nodeId) {
        TaskmeshProperties properties = new TaskmeshProperties();
        properties.setProfile(WorkloadProfile.HIGH_VOLUME);
        properties.setNodeId(nodeId);
        properties.setWorkerPoolSize(WORKERS);
        properties.setBatchSize(BATCH);
        properties.setQueueCapacity(QUEUE);
        properties.setQueryIntervalSeconds(POLL_INTERVAL_SECONDS);
        properties.setMaxRetries(MAX_RETRIES);
        properties.setBacklogPromotion(PROMOTION);
        properties.setInFlightLowWatermark(LOW_WATERMARK);
        properties.setInFlightHighWatermark(HIGH_WATERMARK);
        properties.setBacklogBatchSize(BACKLOG_BATCH);
        properties.setBacklogIntervalSeconds(BACKLOG_INTERVAL);
        return TaskmeshProfileBuilder.profile(WorkloadProfile.HIGH_VOLUME)
                .properties(properties)
                .taskRepository(taskRepository)
                .inFlightRepository(inFlightRepository)
                .historyEventRepository(historyEventRepository)
                .attemptHistoryRepository(attemptHistoryRepository)
                .auditEventRepository(auditEventRepository)
                .incidentRepository(incidentRepository)
                .controlFlagRepository(controlFlagRepository)
                .transactionManager(transactionManager)
                .processors(List.of(okProcessor, failProcessor))
                .payloadProviderFactory(payloadFactory())
                .pauseController(pause)
                // Minutes of backoff would make this a test of Thread.sleep. The retry path itself
                // is what's under measurement, not the exponential curve (covered by its own unit test).
                .retryPolicy(new ExponentialBackoffRetryPolicy(MAX_RETRIES, Duration.ofMillis(1), Duration.ofMillis(1)))
                .metricsReporter(metrics)
                .build();   // build() starts the engine
    }

    @Test
    void aBacklogDrainedByRealProcessorsThroughTheEngineIsTimedAndAccountedFor() throws Exception {
        int total = TASKS + FAIL_TASKS;

        // ---- Phase 1: enqueue, cluster paused ----------------------------------
        Instant enqueueStart = Instant.now();
        runAcrossNodes(node -> {
            for (int i = node; i < TASKS; i += NODES) {
                engines.get(node).enqueue(ref(TYPE_OK, "ok-" + i));
            }
            for (int i = node; i < FAIL_TASKS; i += NODES) {
                engines.get(node).enqueue(ref(TYPE_FAIL, "fail-" + i));
            }
            return null;
        });
        Duration enqueueTook = Duration.between(enqueueStart, Instant.now());

        assertEquals(total, countCold(TaskStatus.QUEUED),
                "the whole backlog must be on disk, and still QUEUED, before the drain starts");
        assertEquals(0, okProcessor.executions.get() + failProcessor.executions.get(),
                "a paused cluster must not have executed anything during enqueue");

        // EAGER's peak is right here — the hot row was written with the cold one, so tm_in_flight
        // already holds the entire backlog before a single task has been claimed.
        long inFlightAfterEnqueue = count("select count(f) from TmInFlightEntity f where f.taskType in :types");
        peakInFlight = Math.max(peakInFlight, inFlightAfterEnqueue);

        // ---- Phase 2: drain ----------------------------------------------------
        Instant drainStart = Instant.now();
        pause.resume();
        long hotLeft = awaitDrain(Duration.ofMinutes(20));
        Duration drainTook = Duration.between(drainStart, Instant.now());

        engines.forEach(TaskMeshEngine::stop);

        long completed = countCold(TaskStatus.COMPLETED);
        long failed = countCold(TaskStatus.FAILED);
        long stillQueued = countCold(TaskStatus.QUEUED);
        long history = count("select count(h) from TmHistoryEventEntity h where h.taskType in :types");
        long incidents = count("select count(i) from TmIncidentEntity i where i.taskType in :types");
        long executions = okProcessor.executions.get() + failProcessor.executions.get();
        double drainSeconds = Math.max(0.001, drainTook.toMillis() / 1000.0);

        System.out.printf("%n[engine-throughput] %s | %s promotion | %d tasks (%d ok + %d always-fail) | %d nodes x %d workers, queue %d, batch %d, %d SHA-256 rounds + 0-%dms random sleep/execution%n"
                        + "  promotion: %s%n"
                        + "  in-flight: after enqueue=%d  peak during drain=%d%n"
                        + "  enqueue:   %.1fs  (%.0f tasks/s)%n"
                        + "  drain:     %.1fs  (%.0f tasks/s, %.0f executions/s)%n"
                        + "  TOTAL:     %.1fs%n"
                        + "  processor: executions=%d  onStart=%d  retried-executions=%d  rerun-signals=%d%n"
                        + "  terminal:  onTerminal COMPLETED=%d FAILED=%d  double-terminal=%d%n"
                        + "  engine:    claimed=%d completed=%d retried=%d failed=%d%n"
                        + "  db:        cold COMPLETED=%d FAILED=%d QUEUED=%d | hot left=%d | history=%d | incidents=%d%n",
                tierName(), PROMOTION, total, TASKS, FAIL_TASKS, NODES, WORKERS, QUEUE, BATCH, WORK_ROUNDS, SLEEP_MILLIS,
                PROMOTION == BacklogPromotion.THROTTLED
                        ? String.format("window %d..%d per node, %d rows every %ds",
                                LOW_WATERMARK, HIGH_WATERMARK, BACKLOG_BATCH, BACKLOG_INTERVAL)
                        : "hot row written eagerly at enqueue",
                inFlightAfterEnqueue, peakInFlight,
                enqueueTook.toMillis() / 1000.0, total / Math.max(0.001, enqueueTook.toMillis() / 1000.0),
                drainSeconds, total / drainSeconds, executions / drainSeconds,
                (enqueueTook.toMillis() + drainTook.toMillis()) / 1000.0,
                executions, okProcessor.starts.get() + failProcessor.starts.get(),
                executions - total, okProcessor.rerunSignals.get() + failProcessor.rerunSignals.get(),
                okProcessor.terminalCompleted.get() + failProcessor.terminalCompleted.get(),
                okProcessor.terminalFailed.get() + failProcessor.terminalFailed.get(),
                okProcessor.doubleTerminal.get() + failProcessor.doubleTerminal.get(),
                metrics.claimed.get(), metrics.completed.get(), metrics.retried.get(), metrics.failed.get(),
                completed, failed, stillQueued, hotLeft, history, incidents);

        // ---- Invariants --------------------------------------------------------
        assertEquals(0, hotLeft, "a terminal outcome deletes the hot row — none may be stranded");
        assertEquals(0, stillQueued, "the backlog must be fully drained");
        assertEquals(total, completed + failed, "every task must end terminal exactly once, and none may be lost");
        assertEquals(0, okProcessor.doubleTerminal.get() + failProcessor.doubleTerminal.get(),
                "onTerminal must fire at most once per task");
        assertEquals(total, okProcessor.terminalCompleted.get() + failProcessor.terminalCompleted.get()
                        + okProcessor.terminalFailed.get() + failProcessor.terminalFailed.get(),
                "onTerminal must fire for every task — a consumer projection that misses one diverges silently");
        assertEquals(0, okProcessor.rerunSignals.get() + failProcessor.rerunSignals.get(),
                "nothing was re-run, so no processor may have seen the D33 rerun signal");

        // The always-failing type is non-retryable, so it goes terminal on its first execution
        // and opens an incident; nothing else here should be producing incidents in bulk.
        assertEquals(FAIL_TASKS, failProcessor.terminalFailed.get(),
                "every always-failing task must reach terminal FAILED exactly once");
        assertEquals(FAIL_TASKS, failProcessor.executions.get(),
                "a non-retryable failure must not be retried");
        assertTrue(incidents >= FAIL_TASKS, "each terminal failure opens an incident: " + incidents);

        // Retryable failures are an independent 1-in-FAIL_ONE_IN roll with a budget of
        // MAX_RETRIES+1 executions, so exhausting one takes that many consecutive failures —
        // vanishingly rare. Anything above a tenth of a percent means the rolls stopped being
        // independent, which would masquerade as the engine over-failing tasks.
        long okFailed = okProcessor.terminalFailed.get();
        assertTrue(okFailed <= Math.max(1, TASKS / 1000),
                "far more exhaustions than " + (MAX_RETRIES + 1) + " independent 1-in-" + FAIL_ONE_IN
                        + " failures can explain: " + okFailed + " of " + TASKS);
        assertTrue(executions > total, "the retry path must actually have been exercised");
        assertEquals(executions, okProcessor.starts.get() + failProcessor.starts.get(),
                "onStart fires exactly once per execution");
        assertTrue(history >= executions, "every execution writes at least a STARTED history event");
    }

    /**
     * Polls until every task is terminal, or the budget runs out, recording the high-water mark of
     * {@code tm_in_flight} on the way — that peak is the whole point of the comparison.
     *
     * <p>"Hot table empty" is <b>not</b> the finish line under {@code THROTTLED}: an unpromoted task
     * has no hot row at all, so the hot table is legitimately empty the instant the drain starts and
     * a hot-rows-only check would report an immediate finish. The backlog is drained when no cold
     * row is still {@code QUEUED} <em>and</em> nothing is left in flight.
     */
    private long awaitDrain(Duration budget) throws InterruptedException {
        long deadline = System.nanoTime() + budget.toNanos();
        while (true) {
            long hot = count("select count(f) from TmInFlightEntity f where f.taskType in :types");
            long queued = countCold(TaskStatus.QUEUED);
            peakInFlight = Math.max(peakInFlight, hot);
            if (hot == 0 && queued == 0) {
                return hot;
            }
            if (System.nanoTime() > deadline) {
                fail("drain did not finish inside " + budget + " — " + hot + " hot rows, " + queued + " still QUEUED");
            }
            Thread.sleep(100);
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

    private void wipe() {
        tx.executeWithoutResult(status -> {
            em.createQuery("DELETE FROM TmInFlightEntity f WHERE f.taskType IN :types").setParameter("types", TYPES).executeUpdate();
            em.createQuery("DELETE FROM TmTaskEntity t WHERE t.taskType IN :types").setParameter("types", TYPES).executeUpdate();
            em.createQuery("DELETE FROM TmHistoryEventEntity h WHERE h.taskType IN :types").setParameter("types", TYPES).executeUpdate();
            em.createQuery("DELETE FROM TmAttemptHistoryEntity a WHERE a.taskType IN :types").setParameter("types", TYPES).executeUpdate();
            em.createQuery("DELETE FROM TmIncidentEntity i WHERE i.taskType IN :types").setParameter("types", TYPES).executeUpdate();
            em.createQuery("DELETE FROM TmAuditEventEntity e WHERE e.taskType IN :types").setParameter("types", TYPES).executeUpdate();
        });
    }

    private long count(String jpql) {
        return tx.execute(status -> em.createQuery(jpql, Long.class).setParameter("types", TYPES).getSingleResult());
    }

    private long countCold(TaskStatus status) {
        return tx.execute(s -> em.createQuery(
                        "select count(t) from TmTaskEntity t where t.taskType in :types and t.status = :status", Long.class)
                .setParameter("types", TYPES).setParameter("status", status).getSingleResult());
    }

    /**
     * D32: at this tier {@code partitionKey} is caller-supplied and <b>required</b>
     * — {@code enqueue()} throws without one and there is no default bucket.
     * Spread over {@value #PARTITIONS} buckets so the run is not accidentally
     * single-partition.
     */
    private TaskRef ref(String type, String id) {
        return new TaskRef(type, id, "P" + (Math.abs(id.hashCode()) % PARTITIONS));
    }

    private String tierName() {
        return "taskmesh-max";
    }

    private PayloadProviderFactory payloadFactory() {
        return taskType -> taskRef -> "payload-for-" + taskRef.taskId();
    }

    /** Flipped by the test to separate the enqueue and drain phases; no DB round trip, no caching. */
    static final class TogglePauseController implements PauseController {
        private volatile boolean paused;

        TogglePauseController(boolean paused) {
            this.paused = paused;
        }

        @Override
        public boolean isPauseRequested() {
            return paused;
        }

        @Override
        public void requestPause() {
            paused = true;
        }

        @Override
        public void resume() {
            paused = false;
        }
    }

    /**
     * A real processor: reads its payload, does a measurable amount of CPU work,
     * and records what the engine told it through both lifecycle hooks.
     */
    static final class MeasuredProcessor implements TaskProcessor {
        private final String taskType;
        private final int workRounds;
        private final int failOneIn;            // 0 = never fail retryably
        private final boolean alwaysFailHard;

        final AtomicLong executions = new AtomicLong();
        final AtomicLong starts = new AtomicLong();
        final AtomicLong terminalCompleted = new AtomicLong();
        final AtomicLong terminalFailed = new AtomicLong();
        final AtomicInteger doubleTerminal = new AtomicInteger();
        final AtomicInteger rerunSignals = new AtomicInteger();
        final AtomicLong sink = new AtomicLong();   // keeps the work from being optimised away
        private final ConcurrentHashMap<String, Integer> terminalPerTask = new ConcurrentHashMap<>();

        MeasuredProcessor(String taskType, int workRounds, int failOneIn, boolean alwaysFailHard) {
            this.taskType = taskType;
            this.workRounds = workRounds;
            this.failOneIn = failOneIn;
            this.alwaysFailHard = alwaysFailHard;
        }

        @Override
        public String taskType() {
            return taskType;
        }

        @Override
        public void onStart(TaskContext ctx) {
            starts.incrementAndGet();
            // D33: the only signal a consumer gets that an operator re-ran a task.
            if (ctx.attemptCount() > 1 && ctx.retryCount() == 0) {
                rerunSignals.incrementAndGet();
            }
        }

        @Override
        public void process(TaskContext ctx) throws Exception {
            executions.incrementAndGet();
            String payload = String.valueOf(ctx.payload());   // exercises the lazy PayloadProvider path
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < workRounds; i++) {
                bytes = digest.digest(bytes);
            }
            sink.addAndGet(bytes[0]);

            if (SLEEP_MILLIS > 0) {
                // Uniform 0..SLEEP_MILLIS rather than a constant: real processors do not all take
                // the same time, and a fixed duration would hide exactly the variance that
                // PollCadence's smoothing exists to absorb.
                Thread.sleep(ThreadLocalRandom.current().nextLong(SLEEP_MILLIS));
            }

            if (alwaysFailHard) {
                throw new NonRetryableException("ALWAYS_FAILS", "this processor never succeeds");
            }
            if (failOneIn > 0 && ThreadLocalRandom.current().nextInt(failOneIn) == 0) {
                // Not a NonRetryableException, so DefaultFailureClassifier calls it retryable.
                throw new IllegalStateException("transient failure under load");
            }
        }

        @Override
        public void onTerminal(TerminalContext ctx) {
            if (ctx.outcome() == TerminalOutcome.COMPLETED) {
                terminalCompleted.incrementAndGet();
            } else {
                terminalFailed.incrementAndGet();
            }
            if (terminalPerTask.merge(ctx.taskRef().taskId(), 1, Integer::sum) > 1) {
                doubleTerminal.incrementAndGet();
            }
        }
    }

    /** Replaces Slf4jMetricsReporter so the engine's own view of the run is countable, not just loggable. */
    static final class CountingMetricsReporter implements MetricsReporter {
        final AtomicLong claimed = new AtomicLong();
        final AtomicLong completed = new AtomicLong();
        final AtomicLong retried = new AtomicLong();
        final AtomicLong failed = new AtomicLong();

        @Override
        public void taskClaimed(String taskType) {
            claimed.incrementAndGet();
        }

        @Override
        public void taskCompleted(String taskType, Duration duration) {
            completed.incrementAndGet();
        }

        @Override
        public void taskRetried(String taskType, String reasonCode) {
            retried.incrementAndGet();
        }

        @Override
        public void taskFailed(String taskType, String reasonCode) {
            failed.incrementAndGet();
        }
    }
}
