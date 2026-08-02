package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.TimeoutPolicy;

import java.time.Duration;
import java.util.Map;

/**
 * Detection-only (Q-TIMEOUT — no preemption mechanism exists). Returns a
 * per-task_type override if configured, otherwise a single default.
 */
public class FixedTimeoutPolicy implements TimeoutPolicy {

    private final Duration defaultTimeout;
    private final Map<String, Duration> overrides;

    public FixedTimeoutPolicy(Duration defaultTimeout) {
        this(defaultTimeout, Map.of());
    }

    public FixedTimeoutPolicy(Duration defaultTimeout, Map<String, Duration> overrides) {
        this.defaultTimeout = defaultTimeout;
        this.overrides = overrides;
    }

    @Override
    public Duration timeoutFor(String taskType) {
        return overrides.getOrDefault(taskType, defaultTimeout);
    }
}
