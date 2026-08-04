package com.palmyralabs.taskmesh.api;

/**
 * Opt-in convenience base for {@link TaskProcessor} implementations that know
 * their payload type at compile time. {@link TaskInfo#payload()} stays
 * {@code Object} at the engine boundary (task_type is a runtime string key,
 * so the engine itself can never statically know the payload type — see
 * docs/planning/decisions.md D2) — this class does the one unavoidable cast
 * in a single place with a clear failure message, instead of every processor
 * author repeating it.
 */
public abstract class TypedTaskProcessor<T> implements TaskProcessor {

    protected abstract Class<T> payloadType();

    protected abstract void process(TaskContext ctx, T payload) throws Exception;

    /**
     * Execution-start hook with the payload already cast — override this rather than
     * {@link #onStart(TaskContext)}. Default no-op; see {@link TaskProcessor#onStart}
     * for how to tell a re-run from a retry.
     */
    protected void onStart(TaskContext ctx, T payload) {
    }

    /**
     * Terminal hook with the payload already cast — override this rather than
     * {@link #onTerminal(TerminalContext)}. Default no-op, and read
     * {@link TaskProcessor#onTerminal} for the transactional contract before
     * implementing it.
     *
     * <p>{@code payload} may be {@code null} here even though {@link #process}
     * guarantees non-null; see the bridge below.
     */
    protected void onTerminal(TerminalContext ctx, T payload) {
    }

    @Override
    public final void process(TaskContext ctx) throws Exception {
        Object payload = ctx.payload();
        if (!payloadType().isInstance(payload)) {
            throw new IllegalStateException(
                    "Task type '" + taskType() + "' expected payload of type "
                            + payloadType().getName() + " but got "
                            + (payload == null ? "null" : payload.getClass().getName()));
        }
        process(ctx, payloadType().cast(payload));
    }

    /**
     * Strict like {@link #process(TaskContext)}, not lenient like
     * {@link #onTerminal(TerminalContext)}: an execution is about to run, and one
     * that cannot resolve its payload cannot do its work either — {@code process}
     * would reject it a moment later anyway, so it is rejected here with the same
     * message. Resolving the payload costs nothing extra: {@code process} loads it
     * immediately afterwards and {@code TaskContext} caches it.
     */
    @Override
    public final void onStart(TaskContext ctx) {
        Object payload = ctx.payload();
        if (!payloadType().isInstance(payload)) {
            throw new IllegalStateException(
                    "Task type '" + taskType() + "' expected payload of type "
                            + payloadType().getName() + " but got "
                            + (payload == null ? "null" : payload.getClass().getName()));
        }
        onStart(ctx, payloadType().cast(payload));
    }

    /**
     * The same single-cast bridge as {@link #process(TaskContext)}, with one
     * deliberate difference: a {@code null} payload is <b>passed through</b>
     * rather than rejected. A task can reach a terminal state without any
     * execution having loaded its payload — an exhausted budget noticed at
     * claim time, for instance — and throwing there would roll back the
     * terminal transition of a task that is otherwise perfectly fine, turning a
     * missing payload into a task that can never finish. A payload of the wrong
     * type is still a programming error and still throws.
     */
    @Override
    public final void onTerminal(TerminalContext ctx) {
        Object payload = ctx.payload();
        if (payload != null && !payloadType().isInstance(payload)) {
            throw new IllegalStateException(
                    "Task type '" + taskType() + "' expected terminal payload of type "
                            + payloadType().getName() + " but got " + payload.getClass().getName());
        }
        onTerminal(ctx, payloadType().cast(payload));
    }
}
