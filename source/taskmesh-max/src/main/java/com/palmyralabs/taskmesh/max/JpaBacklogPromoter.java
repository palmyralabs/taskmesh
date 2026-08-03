package com.palmyralabs.taskmesh.max;

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
                TmInFlightEntity hot = TmTaskEntities.newReservedRow(
                        new TaskRef(task.getTaskType(), task.getTaskId(), task.getPartitionKey()), nodeId);
                inFlightRepository.save(hot);
            }
            List<String> taskIds = backlog.stream().map(TmTaskEntity::getTaskId).toList();
            taskRepository.stampPromoted(taskType, taskIds, now);
            return backlog.size();
        });
        return promoted == null ? 0 : promoted;
    }
}
