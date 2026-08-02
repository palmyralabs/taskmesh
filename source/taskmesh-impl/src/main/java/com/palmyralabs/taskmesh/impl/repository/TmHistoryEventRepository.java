package com.palmyralabs.taskmesh.impl.repository;
import com.palmyralabs.taskmesh.impl.entity.TmHistoryEventEntity;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TmHistoryEventRepository extends JpaRepository<TmHistoryEventEntity, Long> {

    /** Full timeline for a task, in insertion order — id is IDENTITY, so this is chronological. */
    List<TmHistoryEventEntity> findByTaskTypeAndTaskIdOrderByIdAsc(String taskType, String taskId);
}
