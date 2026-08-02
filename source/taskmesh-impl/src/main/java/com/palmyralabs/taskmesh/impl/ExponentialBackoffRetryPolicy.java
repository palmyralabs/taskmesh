package com.palmyralabs.taskmesh.impl;

import com.palmyralabs.taskmesh.api.RetryPolicy;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.Instant;

/**
 * {@code base * 2^(attempt-1)}, capped at {@code cap} — the exact formula
 * from task-execution.md / the HPE-LIC migration toolkit's {@code BackoffCalculator}
 * (default there: base=2min, cap=1h, maxAttempts=10).
 */
@RequiredArgsConstructor
public class ExponentialBackoffRetryPolicy implements RetryPolicy {

    private final int maxAttempts;
    private final Duration base;
    private final Duration cap;

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }

    @Override
    public Instant nextRetryAt(int attemptCount) {
        long factor = 1L << Math.min(attemptCount - 1, 32);
        Duration delay = base.multipliedBy(factor);
        if (delay.compareTo(cap) > 0) {
            delay = cap;
        }
        return Instant.now().plus(delay);
    }
}
