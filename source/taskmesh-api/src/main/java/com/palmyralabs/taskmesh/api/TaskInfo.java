package com.palmyralabs.taskmesh.api;

/**
 * What every task-facing callback knows about the task it was handed:
 * identity, where it is in the D33 counter model, and its payload.
 *
 * <p>Extracted so {@link TaskContext} and {@link TerminalContext} share these
 * accessors rather than declaring them twice. The two contexts stay distinct
 * types on purpose — see {@link TerminalContext}.
 */
public interface TaskInfo {

    TaskRef taskRef();

    /**
     * The attempt's own number — 1 for a freshly enqueued task, incrementing
     * only on an operator {@code rerun} (D33). Unbounded.
     */
    int attemptCount();

    /**
     * The execution's ordinal <i>within</i> that attempt — 0 for the attempt's
     * first execution, incrementing on every claim that starts a new one.
     * Bounded by the retry policy.
     */
    int retryCount();

    /**
     * {@code Object} at the engine boundary: {@code task_type} is a runtime
     * string key, so the engine can never statically know the payload type
     * (D2). {@link TypedTaskProcessor} does the one cast in a single place.
     */
    Object payload();
}
