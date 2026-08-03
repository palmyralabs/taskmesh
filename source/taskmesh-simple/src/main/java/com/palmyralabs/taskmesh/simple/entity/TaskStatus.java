package com.palmyralabs.taskmesh.simple.entity;

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
    COMPLETED,
    FAILED
}
