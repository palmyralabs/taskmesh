package com.palmyralabs.taskmesh.api;

/**
 * Opt-in convenience base for {@link TaskProcessor} implementations that know
 * their payload type at compile time. {@link TaskContext#payload()} stays
 * {@code Object} at the engine boundary (task_type is a runtime string key,
 * so the engine itself can never statically know the payload type — see
 * docs/planning/decisions.md D2) — this class does the one unavoidable cast
 * in a single place with a clear failure message, instead of every processor
 * author repeating it.
 */
public abstract class TypedTaskProcessor<T> implements TaskProcessor {

    protected abstract Class<T> payloadType();

    protected abstract void process(TaskContext ctx, T payload) throws Exception;

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
}
