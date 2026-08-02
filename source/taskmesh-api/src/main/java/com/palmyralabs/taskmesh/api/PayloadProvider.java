package com.palmyralabs.taskmesh.api;

/**
 * Lazily supplies a task's decoded payload, given its identity. The engine
 * never fetches/decodes payload eagerly during discovery (D21) — it only
 * calls {@link #load(TaskRef)} once, from inside {@code TaskContext.payload()},
 * so a task that's discovered but never claimed (or claimed but never reads
 * its payload) never pays for the fetch. The engine obtains a provider per
 * task_type via {@link PayloadProviderFactory} (D23), not one shared
 * instance — how the payload is actually retrieved/decoded (a repository
 * lookup, {@link PayloadCodec}, etc.) is entirely up to the implementation.
 */
@FunctionalInterface
public interface PayloadProvider {

    Object load(TaskRef taskRef) throws Exception;
}
