package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.MetricsReporter;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryMetricsReporter implements MetricsReporter {

    public record Completion(String taskType, Duration duration) {
    }

    public record ReasonedEvent(String taskType, String reasonCode) {
    }

    private final List<String> claimed = new CopyOnWriteArrayList<>();
    private final List<Completion> completions = new CopyOnWriteArrayList<>();
    private final List<ReasonedEvent> retries = new CopyOnWriteArrayList<>();
    private final List<ReasonedEvent> failures = new CopyOnWriteArrayList<>();

    @Override
    public void taskClaimed(String taskType) {
        claimed.add(taskType);
    }

    @Override
    public void taskCompleted(String taskType, Duration duration) {
        completions.add(new Completion(taskType, duration));
    }

    @Override
    public void taskRetried(String taskType, String reasonCode) {
        retries.add(new ReasonedEvent(taskType, reasonCode));
    }

    @Override
    public void taskFailed(String taskType, String reasonCode) {
        failures.add(new ReasonedEvent(taskType, reasonCode));
    }

    public List<String> claimed() {
        return List.copyOf(claimed);
    }

    public List<Completion> completions() {
        return List.copyOf(completions);
    }

    public List<ReasonedEvent> retries() {
        return List.copyOf(retries);
    }

    public List<ReasonedEvent> failures() {
        return List.copyOf(failures);
    }
}
