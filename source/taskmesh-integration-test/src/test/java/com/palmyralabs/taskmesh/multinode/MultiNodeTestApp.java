package com.palmyralabs.taskmesh.multinode;

import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.simple.starter.TaskmeshEngineAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Config for the two-node chaos test. The taskmesh <b>engine</b> auto-config is
 * excluded — the test builds two engines by hand (distinct node ids) sharing
 * the repositories from {@code TaskmeshJpaAutoConfiguration}, so we get real
 * cross-node claim arbitration on one database. Lives in its own sub-package so
 * its component scan doesn't collide with the single-node test app.
 */
@SpringBootApplication(exclude = {
        HibernateJpaAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        TaskmeshEngineAutoConfiguration.class
})
public class MultiNodeTestApp {

    /**
     * Records which node executed each task by reading the worker thread name
     * ({@code taskmesh-<node>-worker-N}). Sleeps briefly so a single node can't
     * instantly drain the whole backlog before the other starts stealing.
     */
    public static class NodeTrackingProcessor implements TaskProcessor {
        private final ConcurrentMap<String, Set<String>> nodesByTask = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> runsByNode = new ConcurrentHashMap<>();

        @Override
        public String taskType() {
            return "MN";
        }

        @Override
        public void process(TaskContext ctx) throws InterruptedException {
            String node = nodeOf(Thread.currentThread().getName());
            nodesByTask.computeIfAbsent(ctx.taskRef().taskId(), k -> ConcurrentHashMap.newKeySet()).add(node);
            runsByNode.computeIfAbsent(node, k -> new AtomicInteger()).incrementAndGet();
            Thread.sleep(50);
        }

        static String nodeOf(String threadName) {
            int worker = threadName.lastIndexOf("-worker-");
            if (threadName.startsWith("taskmesh-") && worker > 0) {
                return threadName.substring("taskmesh-".length(), worker);
            }
            return threadName;
        }

        public int runsBy(String node) {
            AtomicInteger c = runsByNode.get(node);
            return c == null ? 0 : c.get();
        }

        public int totalRuns() {
            return runsByNode.values().stream().mapToInt(AtomicInteger::get).sum();
        }

        /** How many distinct task ids were executed by more than one node (cross-node duplicates). */
        public long tasksRunByMultipleNodes() {
            return nodesByTask.values().stream().filter(s -> s.size() > 1).count();
        }
    }

    /**
     * Simulates a dead worker on node-1: a task claimed by node-1 stalls well past the
     * claim-timeout WITHOUT checkpointing (so its lease is never renewed), while node-2
     * completes quickly. node-2 must recover the stalled claims after the claim-timeout.
     */
    public static class StallOnNode1Processor implements TaskProcessor {
        private final ConcurrentMap<String, AtomicInteger> runsByNode = new ConcurrentHashMap<>();

        @Override
        public String taskType() {
            return "STALL";
        }

        @Override
        public void process(TaskContext ctx) throws InterruptedException {
            String node = NodeTrackingProcessor.nodeOf(Thread.currentThread().getName());
            runsByNode.computeIfAbsent(node, k -> new AtomicInteger()).incrementAndGet();
            Thread.sleep(node.equals("node-1") ? 8000L : 50L);   // node-1 stalls; node-2 finishes fast
        }

        public int runsBy(String node) {
            AtomicInteger c = runsByNode.get(node);
            return c == null ? 0 : c.get();
        }
    }

    @Bean
    public NodeTrackingProcessor nodeTrackingProcessor() {
        return new NodeTrackingProcessor();
    }

    @Bean
    public StallOnNode1Processor stallProcessor() {
        return new StallOnNode1Processor();
    }

    /** Sleeps a random 2–5s; no per-task tracking, to stay memory-light at 50k scale. */
    public static class RandomDelayProcessor implements TaskProcessor {
        private final ConcurrentMap<String, AtomicInteger> runsByNode = new ConcurrentHashMap<>();

        @Override
        public String taskType() {
            return "RND";
        }

        @Override
        public void process(TaskContext ctx) throws InterruptedException {
            Thread.sleep(2000L + java.util.concurrent.ThreadLocalRandom.current().nextLong(3001L));   // 2000–5000ms
            String node = NodeTrackingProcessor.nodeOf(Thread.currentThread().getName());
            runsByNode.computeIfAbsent(node, k -> new AtomicInteger()).incrementAndGet();
        }

        public int runsBy(String node) {
            AtomicInteger c = runsByNode.get(node);
            return c == null ? 0 : c.get();
        }

        public int totalRuns() {
            return runsByNode.values().stream().mapToInt(AtomicInteger::get).sum();
        }
    }

    @Bean
    public RandomDelayProcessor randomDelayProcessor() {
        return new RandomDelayProcessor();
    }

    @Bean
    public PayloadProviderFactory payloadProviderFactory() {
        PayloadProvider provider = ref -> ref.taskId();
        return taskType -> provider;
    }
}
