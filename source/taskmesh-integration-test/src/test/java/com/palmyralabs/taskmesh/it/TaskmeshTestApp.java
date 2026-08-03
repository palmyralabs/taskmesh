package com.palmyralabs.taskmesh.it;

import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.exception.NonRetryableException;
import com.palmyralabs.taskmesh.simple.ExponentialBackoffRetryPolicy;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    /** Fast retry policy so retry/exhaustion scenarios run in seconds, not minutes. maxAttempts=4. */
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
