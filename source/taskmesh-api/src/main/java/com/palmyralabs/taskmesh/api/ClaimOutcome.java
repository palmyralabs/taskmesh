package com.palmyralabs.taskmesh.api;

public enum ClaimOutcome {
    WON,
    LOST,
    /**
     * The claim was won, but the execution it would have started is past the
     * retry budget, so the implementation terminated the task instead of
     * handing it over: the cold {@code tm_task} row was snapshotted
     * {@code FAILED} and the hot row deleted, both fenced by the ownership the
     * claim had just acquired. No {@code release()} follows — there is no
     * longer a claim to release.
     *
     * <p>Exists because retry budget is spent at <b>claim</b> time (D33): a
     * node that loses its lease can never write anything, so if only
     * {@code release()} could terminate a task, one that systematically dies
     * mid-execution would be reclaimed forever and never reach a terminal
     * state. Mirrors {@link ReleaseOutcome#EXHAUSTED} (D19) — the caller must
     * do the same bookkeeping it does there (incident, {@code FAILED}
     * history, {@code taskFailed} metric) rather than treating this as an
     * ordinary lost race.
     */
    EXHAUSTED
}
