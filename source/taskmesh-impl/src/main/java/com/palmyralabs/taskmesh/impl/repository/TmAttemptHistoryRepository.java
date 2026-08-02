package com.palmyralabs.taskmesh.impl.repository;
import com.palmyralabs.taskmesh.impl.entity.TmAttemptHistoryEntity;
import com.palmyralabs.taskmesh.impl.entity.TmAttemptHistoryId;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmAttemptHistoryRepository extends JpaRepository<TmAttemptHistoryEntity, TmAttemptHistoryId> {
}
