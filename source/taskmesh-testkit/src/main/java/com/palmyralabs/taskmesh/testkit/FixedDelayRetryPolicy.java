package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.RetryPolicy;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.Instant;

@RequiredArgsConstructor
public class FixedDelayRetryPolicy implements RetryPolicy {

    private final int maxAttempts;
    private final Duration delay;

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }

    @Override
    public Instant nextRetryAt(int attemptCount) {
        return Instant.now().plus(delay);
    }
}
