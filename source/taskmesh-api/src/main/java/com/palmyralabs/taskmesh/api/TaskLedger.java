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
     * The in-progress attempt's own number for a currently-claimed task —
     * i.e. the persisted hot-row counter, read fresh rather than tracked in
     * engine memory (an in-memory counter doesn't survive a JVM restart, and
     * drifts across nodes). The hot row's {@code attempt_count} is set to
     * this value the moment the row is created (see {@link #rerun}) and
     * bumped again on each successful requeue, so it always reflects
     * "whichever attempt is/was current" — the caller uses it as-is, with no
     * further {@code +1}. Meaningful only for a task this node currently
     * owns, so it's read once, right after a successful
     * {@link ClaimStrategy#claim}. Returns {@code 0} if the task is unknown.
     */
    int attemptCount(TaskRef taskRef);

    /**
     * Operator rerun — resets an existing, currently-terminal task to a
     * fresh {@code QUEUED} state (recreating the hot claim row a terminal
     * outcome deleted) so it is claimed and processed again. A no-op for a
     * task that was never enqueued. Unlike {@link #enqueue}, this is
     * <b>not</b> idempotent-skipping for a terminal task — it resets even one
     * that already reached {@code COMPLETED}/{@code FAILED}; that's the
     * point. The engine layers audit logging and incident resolution on top
     * (see {@code TaskMeshEngine.rerun}).
     *
     * <p><b>Throws {@link IllegalStateException} if a hot row already
     * exists</b>, in any status — a task still {@code CLAIMED} or mid-retry
     * ({@code QUEUED}/{@code RELEASED} with attempts already used) is still
     * alive in the ordinary claim/retry cycle and isn't rerun's to touch;
     * forcing it would also make the recreated row's {@code attempt_count}
     * regress (cold is stale for the entire in-flight/retry lifetime — see
     * below), colliding with attempt-history rows already written for
     * attempts the stale count doesn't know about. If it's genuinely stuck,
     * the normal claim/lease recovery path — not rerun — is what picks it
     * back up.
     *
     * <p>The recreated hot row's {@code attempt_count} is seeded from the
     * cold row's {@code attempt_count} <b>+ 1</b> (cold holds the last
     * attempt that actually ran; the hot row always holds the current one) —
     * never reset to 0, since {@link #attemptCount} feeds
     * {@code HistoryEvent.attemptNumber}, which keys
     * {@code tm_attempt_history}; restarting the counter would make the next
     * attempt's history write collide with, and silently overwrite, the
     * pre-rerun attempt of the same number.
     */
    void rerun(TaskRef taskRef);
}
