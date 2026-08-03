package com.palmyralabs.taskmesh.api;

import java.time.Instant;

/**
 * Controls when a task's hot {@code tm_in_flight} claim row is created relative
 * to its cold {@code tm_task} row written by {@link TaskLedger#enqueue}. The
 * default ({@code NoopBacklogPromoter}) creates it immediately, in {@link
 * #onEnqueue}, reproducing the historical eager-pair behavior. A throttling
 * implementation instead leaves {@link #onEnqueue} a no-op and creates hot
 * rows in bounded batches from {@link #promote}, which the engine calls on its
 * own timer (same driving model as {@link TaskLedger#pullPending}) — this
 * keeps {@code tm_in_flight} bounded to a configured in-flight window instead
 * of growing to match the full backlog when a large batch is enqueued at once.
 */
public interface BacklogPromoter {

    /**
     * Called once, synchronously, immediately after {@code taskRef}'s
     * {@code tm_task} row is written.
     */
    void onEnqueue(TaskRef taskRef, String nodeId);

    /**
     * Called periodically by the engine. Promotes up to {@code limit}
     * still-backlogged tasks of {@code taskType} to hot {@code tm_in_flight}
     * rows and returns how many were promoted.
     */
    int promote(String taskType, Instant now, int limit);
}
