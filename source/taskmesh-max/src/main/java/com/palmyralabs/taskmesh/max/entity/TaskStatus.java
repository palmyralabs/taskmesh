package com.palmyralabs.taskmesh.max.entity;

/**
 * {@code RELEASED} (D27) distinguishes "never claimed" from "claimed then
 * given back for retry" — {@code MultiNodeTaskLedger}'s affinity window
 * keys on it; {@code SimpleTaskLedger} never produces it (retries just go
 * back to {@code QUEUED}, since there's no affinity concept without a
 * cross-node race to arbitrate).
 */
public enum TaskStatus {
    QUEUED,
    CLAIMED,
    RELEASED,
    /**
     * Handed back voluntarily by an owner that still held its lease (D33) —
     * cooperative pause/shutdown. Distinct from {@code QUEUED} because
     * {@code claim} must be able to tell "this task is resuming work it gave
     * back" from "this task is starting an execution": every claim spends one
     * unit of retry budget <b>except</b> a claim of this status.
     */
    YIELDED,
    COMPLETED,
    FAILED
}
