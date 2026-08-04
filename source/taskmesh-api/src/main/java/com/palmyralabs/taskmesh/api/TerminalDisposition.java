package com.palmyralabs.taskmesh.api;

/** How a node hands a claimed task back (D33). */
public enum TerminalDisposition {
    COMPLETED,
    /**
     * Failed and worth another execution — consumes retry budget
     * ({@code retry_count += 1}) and sets the backoff gate. Named for what it
     * is; the old {@code REQUEUE} collided with the operator-initiated
     * {@link TaskLedger#rerun}, which is the conflation D33 removes.
     */
    RETRY,
    /**
     * Unfinished work handed back voluntarily while the lease is still held —
     * cooperative pause/shutdown. Consumes <b>no</b> retry budget and sets
     * <b>no</b> backoff gate: it must be immediately claimable again once
     * whatever caused the yield clears.
     */
    YIELD,
    FAILED_TERMINAL
}
