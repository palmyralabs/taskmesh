package com.palmyralabs.taskmesh.pro.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/** JPA composite-key class for {@link TmTaskEntity}, mirroring TaskRef's (taskType, taskId) identity. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TaskId implements Serializable {
    private String taskType;
    private String taskId;
}
