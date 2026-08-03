package com.palmyralabs.taskmesh.simple;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;

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
    // real gate (Math.min against it in requeueRelease), matching D16.
    static final int UNBOUNDED_MAX_ATTEMPTS = Integer.MAX_VALUE;

    /** Cold ledger row — {@code QUEUED} at enqueue; {@code attempt_count}/{@code max_attempts} are placeholders until the terminal snapshot copies them from the hot row. */
    static TmTaskEntity newQueuedRow(TaskRef taskRef) {
        TmTaskEntity entity = new TmTaskEntity();
        entity.setTaskType(taskRef.taskType());
        entity.setTaskId(taskRef.taskId());
        entity.setStatus(TaskStatus.QUEUED);
        entity.setAttemptCount(0);
        entity.setMaxAttempts(UNBOUNDED_MAX_ATTEMPTS);
        Instant now = Instant.now();
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        return entity;
    }

    /**
     * Hot claim-state row for the {@code enqueue}/{@code execute}/rerun paths —
     * {@code status = QUEUED}, reserved to the enqueuing {@code nodeId} (its
     * reservation window), carrying the live retry counters. A bulk/external
     * populate path bypasses this and inserts an unreserved ({@code status = null},
     * {@code node_id = null}) row directly.
     *
     * <p>{@code attemptCount} is caller-supplied rather than hardcoded — the
     * hot row's counter always represents "the current attempt's own
     * number," seeded as the cold row's {@code attempt_count + 1} (1 for a
     * brand-new task at enqueue, since its cold row is always freshly 0; the
     * carried-forward value at rerun).
     */
    static TmInFlightEntity newReservedRow(TaskRef taskRef, String nodeId, int attemptCount) {
        TmInFlightEntity entity = new TmInFlightEntity();
        entity.setTaskType(taskRef.taskType());
        entity.setTaskId(taskRef.taskId());
        entity.setStatus(TaskStatus.QUEUED);
        entity.setNodeId(nodeId);
        entity.setAttemptCount(attemptCount);
        entity.setMaxAttempts(UNBOUNDED_MAX_ATTEMPTS);
        Instant now = Instant.now();
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        return entity;
    }
}
