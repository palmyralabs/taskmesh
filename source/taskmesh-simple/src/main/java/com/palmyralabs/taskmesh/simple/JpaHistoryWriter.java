package com.palmyralabs.taskmesh.simple;
import com.palmyralabs.taskmesh.simple.entity.TmHistoryEventEntity;
import com.palmyralabs.taskmesh.simple.repository.TmHistoryEventRepository;

import com.palmyralabs.taskmesh.api.HistoryEvent;
import com.palmyralabs.taskmesh.api.HistoryWriter;

/**
 * {@code level} picks which {@link com.palmyralabs.taskmesh.api.HistoryEventType}s
 * actually get persisted — see {@link HistoryLevel} for the profile mapping
 * (BATCH/HIGH_VOLUME/REACTIVE all use this same class, just at different
 * levels, rather than three separate {@code HistoryWriter} implementations).
 */
public class JpaHistoryWriter implements HistoryWriter {

    private final TmHistoryEventRepository repository;
    private final HistoryLevel level;

    public JpaHistoryWriter(TmHistoryEventRepository repository) {
        this(repository, HistoryLevel.VERBOSE);
    }

    public JpaHistoryWriter(TmHistoryEventRepository repository, HistoryLevel level) {
        this.repository = repository;
        this.level = level;
    }

    @Override
    public void record(HistoryEvent event) {
        if (!level.records(event.type())) {
            return;
        }
        TmHistoryEventEntity entity = new TmHistoryEventEntity();
        entity.setTaskType(event.taskRef().taskType());
        entity.setTaskId(event.taskRef().taskId());
        entity.setAttemptCount(event.attemptCount());
        entity.setRetryCount(event.retryCount());
        entity.setEventType(event.type());
        entity.setDetail(event.detail());
        entity.setOccurredAt(event.occurredAt());
        repository.save(entity);
    }
}
