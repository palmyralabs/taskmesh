package com.palmyralabs.taskmesh.max.repository;
import com.palmyralabs.taskmesh.max.entity.TmAttemptHistoryEntity;
import com.palmyralabs.taskmesh.max.entity.TmAttemptHistoryId;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmAttemptHistoryRepository extends JpaRepository<TmAttemptHistoryEntity, TmAttemptHistoryId> {
}
