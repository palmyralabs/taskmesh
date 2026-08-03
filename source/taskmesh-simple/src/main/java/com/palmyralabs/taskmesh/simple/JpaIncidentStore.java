package com.palmyralabs.taskmesh.simple;
import com.palmyralabs.taskmesh.simple.entity.TmIncidentEntity;
import com.palmyralabs.taskmesh.simple.repository.TmIncidentRepository;

import com.palmyralabs.taskmesh.api.Incident;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.TaskRef;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.List;

@RequiredArgsConstructor
public class JpaIncidentStore implements IncidentStore {

    private final TmIncidentRepository repository;

    @Override
    public void open(Incident incident) {
        TmIncidentEntity entity = new TmIncidentEntity();
        entity.setTaskType(incident.taskRef().taskType());
        entity.setTaskId(incident.taskRef().taskId());
        entity.setReasonCode(incident.reasonCode());
        entity.setMessage(incident.message());
        entity.setCreatedAt(incident.createdAt());
        repository.save(entity);
    }

    @Override
    public void resolve(TaskRef taskRef, String resolvedBy) {
        List<TmIncidentEntity> open = repository.findByTaskTypeAndTaskIdAndResolvedAtIsNull(
                taskRef.taskType(), taskRef.taskId());
        Instant now = Instant.now();
        for (TmIncidentEntity incident : open) {
            incident.setResolvedAt(now);
            incident.setResolvedBy(resolvedBy);
        }
        repository.saveAll(open);
    }
}
