package com.palmyralabs.taskmesh.max;

import com.palmyralabs.taskmesh.max.entity.PartitionedTaskId;
import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.TaskRef;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;

/**
 * Default {@link BacklogPromoter} for this tier — unlike {@code enqueue()} in
 * taskmesh-simple/-pro, this tier's {@link PartitionedTaskLedger} only ever
 * writes the cold {@code tm_task} row, so {@link #onEnqueue} is where the
 * matching {@code tm_in_flight} row gets created — immediately, reproducing
 * the historical eager-pair behavior. {@link #promote} is a true no-op:
 * nothing is ever left un-promoted when this is wired instead of
 * {@link JpaBacklogPromoter}.
 */
@RequiredArgsConstructor
public class NoopBacklogPromoter implements BacklogPromoter {

    private final TmTaskRepository taskRepository;
    private final TmInFlightRepository inFlightRepository;
    private final TransactionTemplate transactionTemplate;

    /**
     * Guarded the same way the old inline {@code enqueue()} code was: only
     * create the hot row if one doesn't already exist. {@code save()} on a
     * manually-assigned composite key is an upsert, not insert-or-throw — an
     * unguarded save on a re-enqueue of an already-claimed task would silently
     * reset it back to unclaimed, losing the current owner's claim.
     *
     * <p>{@code attempt_count} is seeded from the cold row's
     * {@code attempt_count + 1} (see {@code TaskLedger#rerun}) — always 1
     * here in practice, since promotion only ever runs for a task that has
     * no hot row yet (i.e. has never been attempted), but read fresh rather
     * than hardcoded so this stays correct if that ever stops holding.
     * {@code retry_count} always starts at 0: promotion opens a new attempt,
     * and the inner counter restarts with every attempt (D33).
     */
    @Override
    public void onEnqueue(TaskRef taskRef, String nodeId) {
        transactionTemplate.executeWithoutResult(status -> {
            TaskId id = new TaskId(taskRef.taskType(), taskRef.taskId());
            if (inFlightRepository.findById(id).isPresent()) {
                return;
            }
            Instant now = Instant.now();
            PartitionedTaskId taskId = new PartitionedTaskId(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey());
            int attemptCount = taskRepository.findById(taskId).map(TmTaskEntity::getAttemptCount).orElse(0) + 1;
            inFlightRepository.save(TmTaskEntities.newReservedRow(taskRef, nodeId, attemptCount, TmTaskEntities.NO_EXECUTION_YET));
            taskRepository.findById(taskId).ifPresent(task -> {
                task.setPromotedAt(now);
                taskRepository.save(task);
            });
        });
    }

    @Override
    public int promote(String taskType, Instant now, int limit) {
        return 0;
    }
}
