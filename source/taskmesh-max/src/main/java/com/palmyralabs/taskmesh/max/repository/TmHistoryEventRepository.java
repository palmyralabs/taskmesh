package com.palmyralabs.taskmesh.max.repository;
import com.palmyralabs.taskmesh.max.entity.TmHistoryEventEntity;
import com.palmyralabs.taskmesh.max.entity.TmHistoryEventId;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TmHistoryEventRepository extends JpaRepository<TmHistoryEventEntity, TmHistoryEventId> {

    /** Full timeline for a task, in insertion order — id is IDENTITY, so this is chronological. */
    List<TmHistoryEventEntity> findByTaskTypeAndTaskIdAndPartitionKeyOrderByIdAsc(
            String taskType, String taskId, String partitionKey);
}
