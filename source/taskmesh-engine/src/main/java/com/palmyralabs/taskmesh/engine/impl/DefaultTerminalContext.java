package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.AttemptCounters;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.api.TerminalContext;
import com.palmyralabs.taskmesh.api.TerminalOutcome;

import java.time.Instant;

/**
 * The engine's {@link TerminalContext}, built at the moment a ledger reports a
 * terminal transition and handed straight to the processor — inside the
 * ledger's transaction.
 *
 * <p>{@code payload} is whatever the execution had <b>already loaded</b>, never
 * a fresh fetch. A terminal can be reached with no execution behind it (a
 * budget found spent at claim time), and loading a payload purely to report an
 * outcome would put consumer-facing I/O inside the terminal transaction to no
 * benefit — so it is simply {@code null} there, which
 * {@link com.palmyralabs.taskmesh.api.TypedTaskProcessor} passes through rather
 * than rejecting.
 */
record DefaultTerminalContext(
        TaskRef taskRef,
        AttemptCounters counters,
        Object payload,
        TerminalOutcome outcome,
        String reasonCode,
        String errorSummary,
        String nodeId,
        Instant terminalAt) implements TerminalContext {

    @Override
    public int attemptCount() {
        return counters.attemptCount();
    }

    @Override
    public int retryCount() {
        return counters.retryCount();
    }
}
