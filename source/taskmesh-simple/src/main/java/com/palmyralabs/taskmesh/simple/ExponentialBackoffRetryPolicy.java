package com.palmyralabs.taskmesh.simple;

import com.palmyralabs.taskmesh.api.Jitter;
import com.palmyralabs.taskmesh.api.RetryPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

/**
 * {@code base * 2^(retry-1)}, capped at {@code cap} — the formula from
 * task-execution.md / the HPE-LIC migration toolkit's {@code BackoffCalculator}
 * (default there: base=2min, cap=1h, 10 retries).
 *
 * <p>Keyed off the inner {@code retry_count} (D33), so the curve restarts with
 * every new attempt — an operator rerun of a task that had already backed off
 * to the cap waits {@code base} again, not {@code cap}.
 *
 * <p>Optional jitter spreads out retries that would otherwise land on the same
 * {@code next_retry_at} instant (e.g. a batch of tasks that all failed together),
 * avoiding a thundering herd across nodes when they become claimable again.
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {


    private final int maxRetries;
    private final Duration base;
    private final Duration cap;
    private final Jitter jitter;
    private final Random random;

    public ExponentialBackoffRetryPolicy(int maxRetries, Duration base, Duration cap) {
        this(maxRetries, base, cap, Jitter.NONE);
    }

    public ExponentialBackoffRetryPolicy(int maxRetries, Duration base, Duration cap, Jitter jitter) {
        this(maxRetries, base, cap, jitter, new Random());
    }

    ExponentialBackoffRetryPolicy(int maxRetries, Duration base, Duration cap, Jitter jitter, Random random) {
        this.maxRetries = maxRetries;
        this.base = base;
        this.cap = cap;
        this.jitter = jitter;
        this.random = random;
    }

    @Override
    public int maxRetries() {
        return maxRetries;
    }

    @Override
    public Instant nextRetryAt(int retryCount) {
        long factor = 1L << Math.min(Math.max(retryCount, 1) - 1, 32);
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
