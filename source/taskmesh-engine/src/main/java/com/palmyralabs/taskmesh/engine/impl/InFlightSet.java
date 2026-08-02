package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.TaskRef;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dedup guard preventing {@code enqueue}/{@code submit}/{@code execute}
 * from pushing the same {@link TaskRef} into the execution queue twice
 * while it's discovered-but-not-yet-claimed (D26) — mirrors palmyra76/workflow's
 * {@code InFlightSet} exactly, including the "lost on JVM crash, self-heals
 * via TaskLedger re-discovery" reasoning: this is per-node, in-memory only,
 * never persisted.
 */
class InFlightSet {

    private final Set<TaskRef> refs = ConcurrentHashMap.newKeySet();

    /** Returns true if newly added (caller should push); false if already in flight. */
    boolean add(TaskRef taskRef) {
        return refs.add(taskRef);
    }

    void remove(TaskRef taskRef) {
        refs.remove(taskRef);
    }

    boolean contains(TaskRef taskRef) {
        return refs.contains(taskRef);
    }
}
