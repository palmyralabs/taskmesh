package com.palmyralabs.taskmesh.impl.repository;
import com.palmyralabs.taskmesh.impl.entity.TmHistoryEventEntity;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmHistoryEventRepository extends JpaRepository<TmHistoryEventEntity, Long> {
}
