package com.palmyralabs.taskmesh.pro.repository;
import com.palmyralabs.taskmesh.pro.entity.TmAttemptHistoryEntity;
import com.palmyralabs.taskmesh.pro.entity.TmAttemptHistoryId;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmAttemptHistoryRepository extends JpaRepository<TmAttemptHistoryEntity, TmAttemptHistoryId> {
}
