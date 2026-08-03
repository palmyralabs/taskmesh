package com.palmyralabs.taskmesh.it;

import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.simple.entity.TaskId;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmHistoryEventEntity;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.simple.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reproduce + diagnose the double-execution. Fires many same-id enqueue bursts
 * (heavy contention on each task's rows) and, for any task that ran more than
 * once, dumps its {@code tm_history_event} timeline. If the double is a
 * completion-path requeue, the timeline shows a RETRIED event (with the
 * failing exception's class as its detail) even though the processor never
 * throws — that's the smoking gun. Output is printed to stdout (captured in the
 * JUnit XML {@code <system-out>}).
 */
@SpringBootTest(properties = {
        "taskmesh.worker-pool-size=16",
        "taskmesh.query-interval-seconds=1",
        "spring.datasource.url=jdbc:h2:mem:reprodb;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=20",
        "spring.datasource.hikari.maximum-pool-size=24"
})
class DoubleExecutionReproTest {

    private static final int ROUNDS = 6;
    private static final int IDS = 40;
    private static final int THREADS_PER_ID = 40;

    @Autowired private TaskMeshEngine engine;
    @Autowired private TaskmeshTestApp.CountingProcessor counting;
    @Autowired private TmTaskRepository taskRepository;
    @Autowired private TmHistoryEventRepository historyRepository;

    @Test
    void reproduceAndDiagnoseDoubleExecution() throws Exception {
        int totalDoubled = 0;
        for (int round = 0; round < ROUNDS; round++) {
            final int r = round;
            List<String> ids = IntStream.range(0, IDS)
                    .mapToObj(i -> "repro-" + r + "-" + i).collect(Collectors.toList());

            ExecutorService pool = Executors.newFixedThreadPool(64);
            CountDownLatch start = new CountDownLatch(1);
            for (String id : ids) {
                TaskRef ref = new TaskRef("OK", id);
                for (int t = 0; t < THREADS_PER_ID; t++) {
                    pool.submit(() -> {
                        try {
                            start.await();
                            engine.enqueue(ref);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "enqueue bursts finished");

            await(() -> ids.stream().allMatch(this::isCompleted), Duration.ofSeconds(90));

            List<String> doubled = ids.stream().filter(id -> counting.count(id) > 1).collect(Collectors.toList());
            totalDoubled += doubled.size();
            System.out.println("REPRO-ROUND " + round + ": " + doubled.size() + "/" + IDS + " task(s) ran more than once");
            for (String id : doubled) {
                String timeline = historyRepository.findByTaskTypeAndTaskIdOrderByIdAsc("OK", id).stream()
                        .map(this::describe)
                        .collect(Collectors.joining(" -> "));
                System.out.println("REPRO-DETAIL " + id + " ran=" + counting.count(id) + " | " + timeline);
            }
        }
        System.out.println("REPRO-SUMMARY: " + totalDoubled + " doubled across " + (ROUNDS * IDS) + " tasks");
    }

    private String describe(TmHistoryEventEntity e) {
        return "att" + e.getAttemptNumber() + ":" + e.getEventType()
                + (e.getDetail() != null ? "(" + e.getDetail() + ")" : "");
    }

    private boolean isCompleted(String id) {
        TmTaskEntity row = taskRepository.findById(new TaskId("OK", id)).orElse(null);
        return row != null && row.getStatus() == TaskStatus.COMPLETED;
    }

    private void await(BooleanSupplier condition, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("not all tasks completed within " + timeout);
    }
}
