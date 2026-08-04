package com.palmyralabs.taskmesh.pro;
import com.palmyralabs.taskmesh.pro.entity.TaskStatus;
import com.palmyralabs.taskmesh.pro.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.pro.entity.TmTaskEntity;

import com.palmyralabs.taskmesh.api.TaskRef;

import java.time.Instant;

/**
 * Shared row-construction helper for {@link SimpleTaskLedger}/{@link MultiNodeTaskLedger}'s
 * {@code enqueue} — builds the matching cold ({@link TmTaskEntity}) and hot
 * ({@link TmInFlightEntity}) rows together, since every enqueued task has
 * both from the moment it's created.
 */
final class TmTaskEntities {

    private TmTaskEntities() {
    }

    // Row-level cap left effectively unbounded — the wired RetryPolicy is the
    // real gate (Math.min against it in retryRelease), matching D16.
    static final int UNBOUNDED_MAX_RETRIES = Integer.MAX_VALUE;

    // A row that has never been claimed has no current execution — the first claim's +1 lands the
    // counter on 0, the first execution's own ordinal (D33). See TmInFlightEntity.retryCount.
    static final int NO_EXECUTION_YET = -1;

    /** Cold ledger row — {@code QUEUED} at enqueue; {@code attempt_count}/{@code retry_count}/{@code max_retries} are placeholders until the terminal snapshot copies them from the hot row. */
    static TmTaskEntity newQueuedRow(TaskRef taskRef) {
        TmTaskEntity entity = new TmTaskEntity();
        entity.setTaskType(taskRef.taskType());
        entity.setTaskId(taskRef.taskId());
        entity.setStatus(TaskStatus.QUEUED);
        entity.setAttemptCount(0);
        entity.setRetryCount(0);
        entity.setMaxRetries(UNBOUNDED_MAX_RETRIES);
        Instant now = Instant.now();
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        return entity;
    }

    /**
     * Hot claim-state row for the {@code enqueue}/{@code execute}/rerun/promote
     * paths — {@code status = QUEUED}, reserved to the enqueuing {@code nodeId}
     * (its reservation window), carrying the live retry counters. A
     * bulk/external populate path bypasses this and inserts an unreserved
     * ({@code status = null}, {@code node_id = null}) row directly.
     *
     * <p>Both counters are caller-supplied rather than hardcoded — the hot
     * row's {@code attempt_count} always represents "the current attempt's own
     * number," seeded as the cold row's {@code attempt_count + 1} (1 for a
     * brand-new task at enqueue, since its cold row is always freshly 0), and
     * {@code retry_count} restarts at {@link #NO_EXECUTION_YET} for every new
     * attempt (D33), so that attempt's first claim bills it up to 0.
     */
    static TmInFlightEntity newReservedRow(TaskRef taskRef, String nodeId, int attemptCount, int retryCount) {
        TmInFlightEntity entity = new TmInFlightEntity();
        entity.setTaskType(taskRef.taskType());
        entity.setTaskId(taskRef.taskId());
        entity.setStatus(TaskStatus.QUEUED);
        entity.setNodeId(nodeId);
        entity.setAttemptCount(attemptCount);
        entity.setRetryCount(retryCount);
        entity.setMaxRetries(UNBOUNDED_MAX_RETRIES);
        Instant now = Instant.now();
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        // Populated at creation, not left null: next_retry_at is the row's "not visible before"
        // instant, and a brand-new row is visible immediately. Behaviourally identical to null —
        // discovery's gate admits both — but it keeps the column populated on every row, which is
        // what lets discovery order by it instead of by created_at.
        entity.setNextRetryAt(now);
        return entity;
    }
}
