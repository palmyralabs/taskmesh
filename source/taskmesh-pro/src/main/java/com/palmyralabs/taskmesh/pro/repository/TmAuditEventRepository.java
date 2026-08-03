package com.palmyralabs.taskmesh.pro.repository;
import com.palmyralabs.taskmesh.pro.entity.TmAuditEventEntity;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmAuditEventRepository extends JpaRepository<TmAuditEventEntity, Long> {
}
