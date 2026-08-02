package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.PayloadStore;
import com.palmyralabs.taskmesh.api.TaskRef;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Map-backed {@link PayloadStore}. Production {@code PayloadStore}/{@code PayloadProvider}
 * implementations are always the consuming project's own (D23) — this fake
 * exists purely so tests can populate payload for a {@link TaskRef} directly
 * without standing up real infra.
 */
public class InMemoryPayloadStore implements PayloadStore {

    private final Map<TaskRef, Object> payloads = new ConcurrentHashMap<>();

    @Override
    public void store(TaskRef taskRef, Object payload) {
        payloads.put(taskRef, payload);
    }

    @Override
    public Object load(TaskRef taskRef) {
        return payloads.get(taskRef);
    }
}
