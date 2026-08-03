package com.palmyralabs.taskmesh.max.repository;
import com.palmyralabs.taskmesh.max.entity.TmIncidentEntity;
import com.palmyralabs.taskmesh.max.entity.TmIncidentId;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface TmIncidentRepository extends JpaRepository<TmIncidentEntity, TmIncidentId> {

    List<TmIncidentEntity> findByTaskTypeAndTaskIdAndPartitionKeyAndResolvedAtIsNull(
            String taskType, String taskId, String partitionKey);

    Optional<TmIncidentEntity> findFirstByTaskTypeAndTaskIdAndPartitionKeyAndResolvedAtIsNull(
            String taskType, String taskId, String partitionKey);
}
