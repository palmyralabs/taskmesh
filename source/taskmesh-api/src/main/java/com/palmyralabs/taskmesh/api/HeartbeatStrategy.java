package com.palmyralabs.taskmesh.api;

/**
 * Node identity is bound internally by the implementation at construction —
 * see D17 in docs/planning/decisions.md.
 */
public interface HeartbeatStrategy {

    RenewOutcome onCheckpoint(TaskRef taskRef);
}
