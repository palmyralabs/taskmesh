package com.palmyralabs.taskmesh.simple.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/** JPA composite-key class for {@link TmAttemptHistoryEntity}. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TmAttemptHistoryId implements Serializable {
    private String taskType;
    private String taskId;
    private int attemptCount;
    private int retryCount;
}
