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

    List<TaskRef> pullPending(Instant now, int limit);

    void enqueue(TaskRef taskRef);

    /**
     * The in-progress execution's own {@code (attempt_count, retry_count)}
     * pair for a currently-claimed task — i.e. the persisted hot-row
     * counters, read fresh rather than tracked in engine memory (an in-memory
     * counter doesn't survive a JVM restart, and drifts across nodes). Both
     * counters are set the moment the hot row is created (see {@link #rerun})
     * and moved on by {@link #rerun} (outer) or by {@link ClaimStrategy#claim}
     * (inner, which is what spends retry budget), so the pair always reflects
     * "whichever execution is/was current" — the caller uses it as-is, with no
     * further {@code +1}. Meaningful only for a task this node currently owns, so
     * it's read once, right after a successful {@link ClaimStrategy#claim}.
     * Returns {@link AttemptCounters#NONE} if the task is unknown.
     */
    AttemptCounters counters(TaskRef taskRef);

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
     * ({@code QUEUED}/{@code RELEASED} with retries already used) is still
     * alive in the ordinary claim/retry cycle and isn't rerun's to touch;
     * forcing it would also make the recreated row's {@code attempt_count}
     * regress (cold is stale for the entire in-flight/retry lifetime). If
     * it's genuinely stuck, the normal claim/lease recovery path — not rerun
     * — is what picks it back up.
     *
     * <p>The recreated hot row's {@code attempt_count} is the cold row's
     * {@code attempt_count} <b>+ 1</b> (a new operator-initiated attempt) and
     * its {@code retry_count} is reset to <b>-1</b> ("no execution yet", so the
     * new attempt's first claim bills it to 0) — the whole point of the D33
     * split: a task that exhausted its retries can be re-run and gets a full
     * fresh retry budget. The reset is only safe because
     * {@code tm_attempt_history} is keyed by <i>both</i> counters, so
     * {@code (1, 3)} and {@code (2, 0)} are distinct rows by construction; a
     * reset against the old single-counter key would have silently
     * overwritten pre-rerun history.
     */
    void rerun(TaskRef taskRef);
}
