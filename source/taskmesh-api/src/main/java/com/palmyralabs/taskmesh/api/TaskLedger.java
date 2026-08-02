package com.palmyralabs.taskmesh.api;

import java.time.Instant;
import java.util.List;

/**
 * The durable record of what work exists and its state — replaces
 * {@code SchedulingStrategy} and {@code TaskQueue} (D22). Implementations
 * are pure data-access classes: the engine owns scheduling, calling
 * {@link #pullPending} on its own timer rather than each implementation
 * self-scheduling. {@link #enqueue} is the one write path both the engine's
 * {@code enqueue()} and {@code execute()} entry points persist through —
 * they differ only in what happens after the write, not in how it's made.
 *
 * <p>Nothing requires work to arrive via {@link #enqueue} — a host may
 * populate the backing storage directly (bypassing this interface's write
 * side entirely); {@link #pullPending} works either way, since it only
 * queries by state, with no awareness of who wrote a given row.
 */
public interface TaskLedger {

    List<TaskRef> pullPending(String taskType, Instant now, int limit);

    void enqueue(TaskRef taskRef);

    /**
     * Operator rerun — unconditionally resets an existing task to a fresh
     * {@code QUEUED} state (recreating the hot claim row if a prior terminal
     * outcome deleted it, and clearing the retry counters) so it is claimed
     * and processed again. A no-op for a task that was never enqueued. Unlike
     * {@link #enqueue}, this is <b>not</b> idempotent-skipping — it resets even
     * a task that already reached a terminal ({@code COMPLETED}/{@code FAILED})
     * outcome; that's the point. The engine layers audit logging and incident
     * resolution on top (see {@code TaskMeshEngine.rerun}).
     */
    void rerun(TaskRef taskRef);
}
