package com.palmyralabs.taskmesh.api;

/**
 * Why a task reached a terminal state, as reported to
 * {@link TaskProcessor#onTerminal}.
 *
 * <p>The two failure values are kept apart even though many consumers map both
 * to one domain state: {@link #FAILED_NONRETRYABLE} is a data problem that
 * re-running will not fix, {@link #FAILED_EXHAUSTED} is infrastructure trouble
 * that an operator rerun probably clears. Collapsing them leaves an
 * undifferentiated failure queue that has to be triaged by hand.
 */
public enum TerminalOutcome {

    /** The processor returned normally and the task was released as done. */
    COMPLETED,

    /**
     * {@code FailureClassifier} judged the failure non-retryable, so the task
     * went terminal on the execution that failed, with budget still unspent.
     */
    FAILED_NONRETRYABLE,

    /**
     * The retry budget ran out — either on a {@code RETRY} release that had
     * nowhere left to go, or at a claim that would have started an execution
     * past the budget (D33; that path exists because a node which loses its
     * lease can never release at all).
     */
    FAILED_EXHAUSTED
}
