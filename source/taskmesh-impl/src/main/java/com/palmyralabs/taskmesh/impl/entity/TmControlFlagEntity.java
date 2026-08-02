package com.palmyralabs.taskmesh.impl.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Single-row cluster pause flag (matches the toolkit's {@code control_flag} table). */
@Entity
@Table(name = "tm_control_flag")
@Getter
@Setter
@NoArgsConstructor
public class TmControlFlagEntity {

    @Id
    @Column(name = "id")
    private Integer id = 1;

    @Column(name = "pause_requested", nullable = false)
    private boolean pauseRequested;
}
