package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.ProgressReporter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryProgressReporter implements ProgressReporter {

    public record ProgressReport(long itemsProcessed, long itemsFailed) {
    }

    private final List<ProgressReport> reports = new ArrayList<>();
    private int heartbeatCount = 0;

    @Override
    public void report(long itemsProcessed, long itemsFailed) {
        reports.add(new ProgressReport(itemsProcessed, itemsFailed));
    }

    @Override
    public void heartbeat() {
        heartbeatCount++;
    }

    public List<ProgressReport> reports() {
        return Collections.unmodifiableList(reports);
    }

    public int heartbeatCount() {
        return heartbeatCount;
    }
}
