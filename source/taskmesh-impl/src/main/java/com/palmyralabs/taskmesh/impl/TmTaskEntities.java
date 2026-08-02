package com.palmyralabs.taskmesh.impl;
import com.palmyralabs.taskmesh.impl.entity.TaskStatus;
import com.palmyralabs.taskmesh.impl.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.impl.entity.TmTaskEntity;

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

    static TmTaskEntity newQueuedRow(TaskRef taskRef) {
        TmTaskEntity entity = new TmTaskEntity();
        entity.setTaskType(taskRef.taskType());
        entity.setTaskId(taskRef.taskId());
        entity.setStatus(TaskStatus.QUEUED);
        entity.setAttemptCount(0);
        // Row-level cap left effectively unbounded — the wired RetryPolicy is
        // the real gate (Math.min against it in requeueRelease), matching D16.
        entity.setMaxAttempts(Integer.MAX_VALUE);
        Instant now = Instant.now();
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        return entity;
    }

    /** {@code status = null} — unclaimed/staged, not yet claimed by anyone. */
    static TmInFlightEntity newUnclaimedRow(TaskRef taskRef) {
        TmInFlightEntity entity = new TmInFlightEntity();
        entity.setTaskType(taskRef.taskType());
        entity.setTaskId(taskRef.taskId());
        Instant now = Instant.now();
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        return entity;
    }
}
