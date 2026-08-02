package com.palmyralabs.taskmesh.api;

/**
 * Opt-in convenience base for {@link PayloadProvider} implementations that
 * know their payload type at compile time — mirrors {@link TypedTaskProcessor}
 * exactly, for the same reason: {@link PayloadProvider#load} stays
 * {@code Object} at the engine boundary ({@link PayloadProviderFactory}
 * looks providers up by a runtime task_type string, so the engine can never
 * statically know {@code T}), but there's no reason provider authors should
 * deal with raw {@code Object}. Unlike {@code TypedTaskProcessor}, no runtime
 * cast/check is needed here — the provider itself produces {@code T}
 * directly, type-safe by construction.
 */
public abstract class TypedPayloadProvider<T> implements PayloadProvider {

    protected abstract T loadTyped(TaskRef taskRef) throws Exception;

    @Override
    public final Object load(TaskRef taskRef) throws Exception {
        return loadTyped(taskRef);
    }
}
