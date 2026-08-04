package com.palmyralabs.taskmesh.api;

import java.time.Instant;

public interface RetryPolicy {

    /**
     * Retries allowed <b>after</b> the initial execution of an attempt, i.e.
     * {@code maxRetries = N} permits N+1 executions per attempt (D33). The
     * bound applies per attempt: {@link TaskLedger#rerun} resets
     * {@code retry_count} to 0, so a re-run gets a full fresh budget.
     */
    int maxRetries();

    /**
     * Backoff gate for the retry about to be scheduled, keyed off the
     * post-increment {@code retry_count} — 1 for the first retry of an
     * attempt. Because it keys off the inner counter, prior re-run history no
     * longer inflates the backoff of a fresh attempt.
     */
    Instant nextRetryAt(int retryCount);
}
