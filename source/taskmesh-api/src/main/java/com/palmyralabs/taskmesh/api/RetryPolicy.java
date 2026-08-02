package com.palmyralabs.taskmesh.api;

import java.time.Instant;

public interface RetryPolicy {

    int maxAttempts();

    Instant nextRetryAt(int attemptCount);
}
