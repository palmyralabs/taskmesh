package com.palmyralabs.taskmesh.simple.repository;
import com.palmyralabs.taskmesh.simple.entity.TmAuditEventEntity;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmAuditEventRepository extends JpaRepository<TmAuditEventEntity, Long> {
}
