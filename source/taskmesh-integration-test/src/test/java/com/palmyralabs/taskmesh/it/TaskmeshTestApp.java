package com.palmyralabs.taskmesh.it;

import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TerminalContext;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.exception.NonRetryableException;
import com.palmyralabs.taskmesh.simple.ExponentialBackoffRetryPolicy;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Boots the full engine via the taskmesh Spring Boot starter against H2.
 * Spring Boot's own Hibernate/JPA + datasource-transaction auto-config is
 * excluded so the ONLY EntityManagerFactory / PlatformTransactionManager in
 * play are taskmesh's own (from {@code TaskmeshJpaAutoConfiguration}) — the
 * app just needs a {@link javax.sql.DataSource} (H2, from
 * {@code DataSourceAutoConfiguration}) plus a processor list and a payload factory.
 */
@SpringBootApplication(exclude = {HibernateJpaAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
public class TaskmeshTestApp {

    /** Succeeds; records how many times each task_id was processed. */
    public static class CountingProcessor implements TaskProcessor {
        private final ConcurrentMap<String, AtomicInteger> counts = new ConcurrentHashMap<>();

        @Override
        public String taskType() {
            return "OK";
        }

        @Override
        public void process(TaskContext ctx) {
            counts.computeIfAbsent(ctx.taskRef().taskId(), k -> new AtomicInteger()).incrementAndGet();
        }

        public int count(String taskId) {
            AtomicInteger c = counts.get(taskId);
            return c == null ? 0 : c.get();
        }
    }

    /** Always throws non-retryably — drives the terminal FAILED + incident path. */
    public static class FailingProcessor implements TaskProcessor {
        @Override
        public String taskType() {
            return "FAIL";
        }

        @Override
        public void process(TaskContext ctx) {
            throw new NonRetryableException("BOOM", "always fails");
        }
    }

    /**
     * Multi-step, long-running (>= 15s) processor that cooperates with the
     * engine: it does work in {@value #STEPS} steps of {@value #STEP_MILLIS}ms,
     * calling {@code ctx.checkpoint()} between steps to renew the lease and
     * refresh ownership, and stops early if it ever loses ownership. Used to
     * exercise many concurrent long tasks with active heartbeating.
     */
    public static class SlowMultiStepProcessor implements TaskProcessor {
        static final int STEPS = 15;
        static final long STEP_MILLIS = 1000L;

        private final java.util.Set<String> completed = ConcurrentHashMap.newKeySet();

        @Override
        public String taskType() {
            return "SLOW";
        }

        @Override
        public void process(TaskContext ctx) throws Exception {
            for (int step = 0; step < STEPS; step++) {
                Thread.sleep(STEP_MILLIS);   // simulate this step's work
                ctx.checkpoint();            // renew lease + refresh ownership between steps
                if (!ctx.isOwnershipValid()) {
                    return;                  // cooperative stop — another node took over
                }
            }
            completed.add(ctx.taskRef().taskId());
        }

        public int completedCount() {
            return completed.size();
        }
    }

    @Bean
    public CountingProcessor countingProcessor() {
        return new CountingProcessor();
    }

    @Bean
    public FailingProcessor failingProcessor() {
        return new FailingProcessor();
    }

    /** Fails retryably the first N times per task_id, then succeeds — exercises the requeue/backoff path. */
    public static class FlakyProcessor implements TaskProcessor {
        private final int failuresBeforeSuccess;
        private final ConcurrentMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

        public FlakyProcessor(int failuresBeforeSuccess) {
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public String taskType() {
            return "FLAKY";
        }

        @Override
        public void process(TaskContext ctx) {
            int n = attempts.computeIfAbsent(ctx.taskRef().taskId(), k -> new AtomicInteger()).incrementAndGet();
            if (n <= failuresBeforeSuccess) {
                throw new RuntimeException("transient failure #" + n);   // retryable by default
            }
        }

        public int attempts(String taskId) {
            AtomicInteger a = attempts.get(taskId);
            return a == null ? 0 : a.get();
        }
    }

    /** Always throws a retryable exception — should exhaust its retry budget and land FAILED + incident. */
    public static class RetryFailProcessor implements TaskProcessor {
        private final AtomicInteger totalAttempts = new AtomicInteger();

        @Override
        public String taskType() {
            return "RETRYFAIL";
        }

        @Override
        public void process(TaskContext ctx) {
            totalAttempts.incrementAndGet();
            throw new RuntimeException("always transient");
        }

        public int totalAttempts() {
            return totalAttempts.get();
        }
    }

    @Bean
    public SlowMultiStepProcessor slowMultiStepProcessor() {
        return new SlowMultiStepProcessor();
    }

    @Bean
    public FlakyProcessor flakyProcessor() {
        return new FlakyProcessor(2);
    }

    @Bean
    public RetryFailProcessor retryFailProcessor() {
        return new RetryFailProcessor();
    }


    /**
     * Records the {@code (attemptCount, retryCount)} pair the engine hands it on
     * every execution, and fails retryably until told to stop — so a test can
     * drive a task through exhaustion, re-run it, and check the counters the
     * <b>engine</b> observed rather than only what the ledger stored (D33).
     */
    public static class CounterRecordingProcessor implements TaskProcessor {
        // Keyed by task_id, and failure is opt-out per task_id, so tests sharing this bean via the
        // cached Spring context stay order-independent.
        private final ConcurrentMap<String, List<String>> observed = new ConcurrentHashMap<>();
        private final Set<String> succeeding = ConcurrentHashMap.newKeySet();

        @Override
        public String taskType() {
            return "COUNTERS";
        }

        @Override
        public void process(TaskContext ctx) {
            String id = ctx.taskRef().taskId();
            observed.computeIfAbsent(id, k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(ctx.attemptCount() + ":" + ctx.retryCount());
            if (!succeeding.contains(id)) {
                throw new RuntimeException("transient by design");
            }
        }

        /** The pairs seen for this task so far, as "attempt:retry", in execution order. */
        public List<String> observedPairs(String taskId) {
            List<String> seen = observed.get(taskId);
            if (seen == null) {
                return List.of();
            }
            synchronized (seen) {
                return List.copyOf(seen);
            }
        }

        public void stopFailing(String taskId) {
            succeeding.add(taskId);
        }
    }

    @Bean
    public CounterRecordingProcessor counterRecordingProcessor() {
        return new CounterRecordingProcessor();
    }


    /**
     * Drives every terminal shape on demand and records what {@code onTerminal}
     * was told, so a test can check both that it fires where it should and that
     * it stays silent where it should not. Behaviour is per task_id so tests
     * sharing this bean via the cached Spring context stay order-independent.
     */
    public static class TerminalRecordingProcessor implements TaskProcessor {

        /** What {@link #process} should do for a given task id. */
        public enum Mode { SUCCEED, FAIL_RETRYABLE, FAIL_NONRETRYABLE, BLOCK_UNTIL_RELEASED }

        private final ConcurrentMap<String, Mode> modes = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> executions = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, List<String>> terminals = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, List<String>> starts = new ConcurrentHashMap<>();
        private final Set<String> failStart = ConcurrentHashMap.newKeySet();
        private final ConcurrentMap<String, Object> terminalPayloads = new ConcurrentHashMap<>();
        /** Task ids whose onTerminal should throw, and how many more times. */
        private final ConcurrentMap<String, AtomicInteger> terminalFailures = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CountDownLatch> started = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CountDownLatch> release = new ConcurrentHashMap<>();

        @Override
        public String taskType() {
            return "TERMINAL";
        }

        public void mode(String taskId, Mode mode) {
            modes.put(taskId, mode);
        }

        /** Make onTerminal throw the next {@code times} times it fires for this task. */
        public void failTerminalTimes(String taskId, int times) {
            terminalFailures.put(taskId, new AtomicInteger(times));
        }

        @Override
        public void onStart(TaskContext ctx) {
            String id = ctx.taskRef().taskId();
            starts.computeIfAbsent(id, k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(ctx.attemptCount() + ":" + ctx.retryCount());
            if (failStart.remove(id)) {
                throw new IllegalStateException("start hook refused " + id);
            }
        }

        /** Make the next onStart for this task throw, once. */
        public void failNextStart(String taskId) {
            failStart.add(taskId);
        }

        /** Execution starts seen for this task, as "attempt:retry", in order. */
        public List<String> startsFor(String taskId) {
            List<String> seen = starts.get(taskId);
            if (seen == null) {
                return List.of();
            }
            synchronized (seen) {
                return List.copyOf(seen);
            }
        }

        @Override
        public void process(TaskContext ctx) {
            String id = ctx.taskRef().taskId();
            int n = executions.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            switch (modes.getOrDefault(id, Mode.SUCCEED)) {
                case SUCCEED -> { }
                case FAIL_RETRYABLE -> throw new RuntimeException("transient by design");
                case FAIL_NONRETRYABLE -> throw new NonRetryableException("PERMANENT", "permanent by design");
                case BLOCK_UNTIL_RELEASED -> {
                    // Parks mid-execution so a test can steal the claim underneath it, which is the
                    // only way to reach the LOST_OWNERSHIP release path from outside the engine.
                    started.computeIfAbsent(id, k -> new CountDownLatch(1)).countDown();
                    try {
                        if (!release.computeIfAbsent(id, k -> new CountDownLatch(1)).await(30, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("test never released " + id);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    if (n < 0) {
                        throw new IllegalStateException("unreachable");
                    }
                }
            }
        }

        @Override
        public void onTerminal(TerminalContext ctx) {
            String id = ctx.taskRef().taskId();
            AtomicInteger remaining = terminalFailures.get(id);
            if (remaining != null && remaining.getAndDecrement() > 0) {
                // Vetoes the terminal transition — the whole transaction, ledger writes included,
                // must roll back and the task must come round again.
                throw new IllegalStateException("projection write refused for " + id);
            }
            terminals.computeIfAbsent(id, k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(ctx.outcome() + "/" + ctx.attemptCount() + ":" + ctx.retryCount());
            if (ctx.payload() != null) {
                terminalPayloads.put(id, ctx.payload());
            }
        }

        /** Terminal notifications seen for this task, as "OUTCOME/attempt:retry". */
        public List<String> terminalsFor(String taskId) {
            List<String> seen = terminals.get(taskId);
            if (seen == null) {
                return List.of();
            }
            synchronized (seen) {
                return List.copyOf(seen);
            }
        }

        /** Blocks until this task's execution has actually begun. */
        public void awaitStarted(String taskId, Duration timeout) throws InterruptedException {
            if (!started.computeIfAbsent(taskId, k -> new CountDownLatch(1)).await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("execution of " + taskId + " never started");
            }
        }

        /** Lets a parked execution finish. */
        public void releaseExecution(String taskId) {
            release.computeIfAbsent(taskId, k -> new CountDownLatch(1)).countDown();
        }

        public int executions(String taskId) {
            AtomicInteger n = executions.get(taskId);
            return n == null ? 0 : n.get();
        }

        public Object terminalPayload(String taskId) {
            return terminalPayloads.get(taskId);
        }
    }

    @Bean
    public TerminalRecordingProcessor terminalRecordingProcessor() {
        return new TerminalRecordingProcessor();
    }

    /** Fast retry policy so retry/exhaustion scenarios run in seconds, not minutes. maxRetries=4. */
    @Bean
    public RetryPolicy retryPolicy() {
        return new ExponentialBackoffRetryPolicy(4, Duration.ofMillis(50), Duration.ofMillis(100));
    }

    /** Trivial payload factory for all task types — the processors don't read payload. */
    @Bean
    public PayloadProviderFactory payloadProviderFactory() {
        PayloadProvider provider = ref -> ref.taskId();
        return taskType -> provider;
    }
}
