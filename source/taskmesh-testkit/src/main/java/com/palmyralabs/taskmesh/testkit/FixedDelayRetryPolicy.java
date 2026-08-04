package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.RetryPolicy;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.Instant;

@RequiredArgsConstructor
public class FixedDelayRetryPolicy implements RetryPolicy {

    private final int maxRetries;
    private final Duration delay;

    @Override
    public int maxRetries() {
        return maxRetries;
    }

    @Override
    public Instant nextRetryAt(int retryCount) {
        return Instant.now().plus(delay);
    }
}
