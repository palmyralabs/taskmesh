package com.palmyralabs.taskmesh.max;

import com.palmyralabs.taskmesh.api.Jitter;
import com.palmyralabs.taskmesh.api.RetryPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

/**
 * {@code base * 2^(attempt-1)}, capped at {@code cap} — the exact formula
 * from task-execution.md / the HPE-LIC migration toolkit's {@code BackoffCalculator}
 * (default there: base=2min, cap=1h, maxAttempts=10).
 *
 * <p>Optional jitter spreads out retries that would otherwise land on the same
 * {@code next_retry_at} instant (e.g. a batch of tasks that all failed together),
 * avoiding a thundering herd across nodes when they become claimable again.
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {


    private final int maxAttempts;
    private final Duration base;
    private final Duration cap;
    private final Jitter jitter;
    private final Random random;

    public ExponentialBackoffRetryPolicy(int maxAttempts, Duration base, Duration cap) {
        this(maxAttempts, base, cap, Jitter.NONE);
    }

    public ExponentialBackoffRetryPolicy(int maxAttempts, Duration base, Duration cap, Jitter jitter) {
        this(maxAttempts, base, cap, jitter, new Random());
    }

    ExponentialBackoffRetryPolicy(int maxAttempts, Duration base, Duration cap, Jitter jitter, Random random) {
        this.maxAttempts = maxAttempts;
        this.base = base;
        this.cap = cap;
        this.jitter = jitter;
        this.random = random;
    }

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
        return Instant.now().plus(applyJitter(delay));
    }

    private Duration applyJitter(Duration delay) {
        return switch (jitter) {
            case NONE -> delay;
            case FULL -> Duration.ofMillis(randomMillis(delay.toMillis()));
            case EQUAL -> {
                long half = delay.toMillis() / 2;
                yield Duration.ofMillis(half + randomMillis(half));
            }
        };
    }

    private long randomMillis(long boundMillis) {
        return boundMillis <= 0 ? 0 : (long) (random.nextDouble() * boundMillis);
    }
}
