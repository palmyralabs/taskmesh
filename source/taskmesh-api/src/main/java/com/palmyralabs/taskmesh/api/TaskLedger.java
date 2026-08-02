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
}
