package com.palmyralabs.taskmesh.max;

import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.max.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.TaskRef;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;

/**
 * Throttled {@link BacklogPromoter} — {@link #onEnqueue} is a no-op (this
 * tier's {@code enqueue()} only ever writes {@code tm_task}), so a large
 * batch enqueued at once leaves {@code tm_in_flight} untouched until
 * {@link #promote} — called periodically by the engine, per {@code
 * taskType} — creates hot rows for the oldest still-backlogged tasks, up to
 * {@code limit} per cycle, keeping {@code tm_in_flight}'s size bounded to
 * the promotion rate rather than the full backlog.
 */
@RequiredArgsConstructor
public class JpaBacklogPromoter implements BacklogPromoter {

    private final TmTaskRepository taskRepository;
    private final TmInFlightRepository inFlightRepository;
    private final TransactionTemplate transactionTemplate;
    private final String nodeId;

    @Override
    public void onEnqueue(TaskRef taskRef, String nodeId) {
        // no-op — hot-row creation happens later, in bounded batches, from promote().
    }

    @Override
    public int promote(String taskType, Instant now, int limit) {
        Integer promoted = transactionTemplate.execute(status -> {
            List<TmTaskEntity> backlog = taskRepository.findUnpromoted(taskType, PageRequest.of(0, limit));
            if (backlog.isEmpty()) {
                return 0;
            }
            for (TmTaskEntity task : backlog) {
                // Same guard NoopBacklogPromoter carries, for the same reason: save() on a
                // manually-assigned composite key is an upsert, not insert-or-throw, so an
                // unguarded save over an existing hot row silently resets a claimed task to
                // unclaimed and loses the current owner's claim. "Unpromoted" normally implies
                // "no hot row" — promotion stamps promoted_at in this very transaction — but it is
                // one missed stamp away from not implying it, and the failure is silent.
                if (inFlightRepository.findById(new TaskId(task.getTaskType(), task.getTaskId())).isPresent()) {
                    continue;
                }
                // Seeded from the cold row's attempt_count + 1 (see TaskLedger#rerun) — always 1 in
                // practice here too, since a backlogged task by definition has no hot row yet.
                // retry_count starts at -1 (the first claim bills it to 0): a new attempt always restarts the inner counter (D33).
                TmInFlightEntity hot = TmTaskEntities.newReservedRow(
                        new TaskRef(task.getTaskType(), task.getTaskId(), task.getPartitionKey()), nodeId,
                        task.getAttemptCount() + 1, TmTaskEntities.NO_EXECUTION_YET);
                inFlightRepository.save(hot);
            }
            List<String> taskIds = backlog.stream().map(TmTaskEntity::getTaskId).toList();
            taskRepository.stampPromoted(taskType, taskIds, now);
            return backlog.size();
        });
        return promoted == null ? 0 : promoted;
    }
}
