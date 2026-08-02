package com.palmyralabs.taskmesh.starter;

import com.palmyralabs.taskmesh.api.MetricsReporter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

/** Zero-config default {@link MetricsReporter} — log-only, matches infra-implementations.md's NoOpMetricsReporter role. */
@Slf4j
public class Slf4jMetricsReporter implements MetricsReporter {

    @Override
    public void taskClaimed(String taskType) {
        log.debug("{} claimed", taskType);
    }

    @Override
    public void taskCompleted(String taskType, Duration duration) {
        log.info("{} completed in {}", taskType, duration);
    }

    @Override
    public void taskRetried(String taskType, String reasonCode) {
        log.warn("{} retried, reason={}", taskType, reasonCode);
    }

    @Override
    public void taskFailed(String taskType, String reasonCode) {
        log.error("{} failed terminally, reason={}", taskType, reasonCode);
    }
}
