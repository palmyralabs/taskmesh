package com.palmyralabs.taskmesh.simple.repository;
import com.palmyralabs.taskmesh.simple.entity.TmAttemptHistoryEntity;
import com.palmyralabs.taskmesh.simple.entity.TmAttemptHistoryId;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmAttemptHistoryRepository extends JpaRepository<TmAttemptHistoryEntity, TmAttemptHistoryId> {
}
