package com.palmyralabs.taskmesh.api;

import java.time.Duration;

/**
 * Enforcement is detection, not preemption — see docs/planning/open-questions.md
 * (Q-TIMEOUT). A task exceeding this duration is flagged, not forcibly killed.
 */
public interface TimeoutPolicy {

    Duration timeoutFor(String taskType);
}
