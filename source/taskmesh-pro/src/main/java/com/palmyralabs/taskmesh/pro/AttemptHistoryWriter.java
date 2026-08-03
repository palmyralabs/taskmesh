package com.palmyralabs.taskmesh.pro;
import com.palmyralabs.taskmesh.pro.entity.TmAttemptHistoryEntity;
import com.palmyralabs.taskmesh.pro.entity.TmAttemptHistoryId;
import com.palmyralabs.taskmesh.pro.repository.TmAttemptHistoryRepository;

import com.palmyralabs.taskmesh.api.HistoryEvent;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import com.palmyralabs.taskmesh.api.HistoryWriter;
import lombok.RequiredArgsConstructor;

/**
 * BATCH profile's {@code HistoryWriter} (infra-implementations.md Variant A,
 * cleanser-style {@code exec_proc_run} pattern) — one row per attempt,
 * INSERTed on {@link HistoryEventType#STARTED}, UPDATEd in place on the
 * attempt's terminal outcome. {@code RETRIED}/{@code OWNERSHIP_LOST}/
 * {@code PAUSED}/{@code HEARTBEAT} update the same row's {@code detail}
 * without changing {@code status} to keep the "coarse, one row is enough"
 * shape — only {@code COMPLETED}/{@code FAILED} close the row ({@code ended_at}).
 */
@RequiredArgsConstructor
public class AttemptHistoryWriter implements HistoryWriter {

    private final TmAttemptHistoryRepository repository;

    @Override
    public void record(HistoryEvent event) {
        TmAttemptHistoryId id = new TmAttemptHistoryId(event.taskRef().taskType(), event.taskRef().taskId(), event.attemptNumber());

        if (event.type() == HistoryEventType.STARTED) {
            TmAttemptHistoryEntity entity = new TmAttemptHistoryEntity();
            entity.setTaskType(id.getTaskType());
            entity.setTaskId(id.getTaskId());
            entity.setAttemptNumber(id.getAttemptNumber());
            entity.setStatus(HistoryEventType.STARTED);
            entity.setStartedAt(event.occurredAt());
            entity.setNodeId(event.nodeId());
            repository.save(entity);
            return;
        }

        repository.findById(id).ifPresentOrElse(entity -> {
            entity.setStatus(event.type());
            entity.setDetail(event.detail());
            entity.setNodeId(event.nodeId());
            if (isTerminal(event.type())) {
                entity.setEndedAt(event.occurredAt());
            }
            repository.save(entity);
        }, () -> {
            // No STARTED row seen for this attempt (e.g. NO_PROCESSOR fails before STARTED is ever recorded) — create one now.
            TmAttemptHistoryEntity entity = new TmAttemptHistoryEntity();
            entity.setTaskType(id.getTaskType());
            entity.setTaskId(id.getTaskId());
            entity.setAttemptNumber(id.getAttemptNumber());
            entity.setStatus(event.type());
            entity.setStartedAt(event.occurredAt());
            entity.setDetail(event.detail());
            entity.setNodeId(event.nodeId());
            if (isTerminal(event.type())) {
                entity.setEndedAt(event.occurredAt());
            }
            repository.save(entity);
        });
    }

    private static boolean isTerminal(HistoryEventType type) {
        return type == HistoryEventType.COMPLETED || type == HistoryEventType.FAILED;
    }
}
