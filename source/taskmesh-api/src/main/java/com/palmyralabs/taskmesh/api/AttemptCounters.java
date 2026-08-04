package com.palmyralabs.taskmesh.api;

/**
 * The two nested execution counters a task carries (D33), read as a pair:
 * {@code (attempt 2, retry 3)} is "the third retry of the second attempt".
 *
 * <ul>
 *   <li>{@code attemptCount} — <b>outer</b>, 1-based. An <i>attempt</i> is one
 *       operator-initiated run: a freshly enqueued task is attempt 1, and
 *       {@link TaskLedger#rerun} increments it. Unbounded — there is no
 *       maximum for a manual action.</li>
 *   <li>{@code retryCount} — <b>inner</b>, 0-based. The ordinal of the
 *       execution currently running within this attempt. Spent by
 *       {@code claim}: every claim bills one unit except one resuming a
 *       yielded task, so an execution whose node died still costs budget even
 *       though it never reported anything. Bounded by {@code max_retries}, and
 *       reset whenever {@code attemptCount} increments.</li>
 * </ul>
 *
 * <p>A {@code retryCount} of {@code -1} means "no execution has started yet" —
 * the state a freshly enqueued or freshly re-run task sits in until its first
 * claim bills it up to 0. Callers read this pair right after a successful
 * claim, so they never see it.
 */
public record AttemptCounters(int attemptCount, int retryCount) {

    /** What a {@link TaskLedger} returns for a task it doesn't know about. */
    public static final AttemptCounters NONE = new AttemptCounters(0, 0);
}
