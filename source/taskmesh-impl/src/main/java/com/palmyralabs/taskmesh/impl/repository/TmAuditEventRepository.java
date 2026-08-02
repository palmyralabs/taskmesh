package com.palmyralabs.taskmesh.impl.repository;
import com.palmyralabs.taskmesh.impl.entity.TmAuditEventEntity;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmAuditEventRepository extends JpaRepository<TmAuditEventEntity, Long> {
}
