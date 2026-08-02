package com.palmyralabs.taskmesh.api;

import java.time.Duration;

public interface MetricsReporter {

    void taskClaimed(String taskType);

    void taskCompleted(String taskType, Duration duration);

    void taskRetried(String taskType, String reasonCode);

    void taskFailed(String taskType, String reasonCode);
}
