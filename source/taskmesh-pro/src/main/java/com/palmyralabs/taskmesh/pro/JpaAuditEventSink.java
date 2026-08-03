package com.palmyralabs.taskmesh.pro;
import com.palmyralabs.taskmesh.pro.entity.TmAuditEventEntity;
import com.palmyralabs.taskmesh.pro.repository.TmAuditEventRepository;

import com.palmyralabs.taskmesh.api.AuditEvent;
import com.palmyralabs.taskmesh.api.AuditEventSink;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class JpaAuditEventSink implements AuditEventSink {

    private final TmAuditEventRepository repository;

    @Override
    public void record(AuditEvent event) {
        TmAuditEventEntity entity = new TmAuditEventEntity();
        entity.setEventType(event.eventType());
        if (event.taskRef() != null) {
            entity.setTaskType(event.taskRef().taskType());
            entity.setTaskId(event.taskRef().taskId());
        }
        entity.setActorType(event.actorType());
        entity.setPerformedBy(event.performedBy());
        entity.setMessage(event.message());
        entity.setOccurredAt(event.occurredAt());
        repository.save(entity);
    }
}
