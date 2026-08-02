package com.palmyralabs.taskmesh.api;

/**
 * Supplies the {@link PayloadProvider} for a given task_type. A single
 * un-keyed provider is wrong for a deployment running multiple task types
 * with different payload sources (D23) — the engine holds one factory
 * instance, looked up per {@code taskRef.taskType()} whenever it needs to
 * resolve a task's payload.
 */
public interface PayloadProviderFactory {

    PayloadProvider getProvider(String taskType);
}
