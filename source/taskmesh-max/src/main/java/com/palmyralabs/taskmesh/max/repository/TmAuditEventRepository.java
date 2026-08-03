package com.palmyralabs.taskmesh.max.repository;
import com.palmyralabs.taskmesh.max.entity.TmAuditEventEntity;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmAuditEventRepository extends JpaRepository<TmAuditEventEntity, Long> {
}
