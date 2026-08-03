package com.palmyralabs.taskmesh.pro.repository;
import com.palmyralabs.taskmesh.pro.entity.TmIncidentEntity;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface TmIncidentRepository extends JpaRepository<TmIncidentEntity, Long> {

    List<TmIncidentEntity> findByTaskTypeAndTaskIdAndResolvedAtIsNull(String taskType, String taskId);

    Optional<TmIncidentEntity> findFirstByTaskTypeAndTaskIdAndResolvedAtIsNull(String taskType, String taskId);
}
