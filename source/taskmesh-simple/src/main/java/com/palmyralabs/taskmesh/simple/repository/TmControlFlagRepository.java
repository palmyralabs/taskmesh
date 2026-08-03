package com.palmyralabs.taskmesh.simple.repository;
import com.palmyralabs.taskmesh.simple.entity.TmControlFlagEntity;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TmControlFlagRepository extends JpaRepository<TmControlFlagEntity, Integer> {
}
