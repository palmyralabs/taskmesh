package com.palmyralabs.taskmesh.max.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * JPA composite-key class for {@link TmTaskEntity} only — {@code
 * partition_key} is required here (Postgres requires the partition column in
 * every unique constraint on a physically-partitioned table), but is
 * deliberately NOT part of {@link TmInFlightEntity}'s key ({@link TaskId}),
 * which is never physically partitioned (D32).
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class PartitionedTaskId implements Serializable {
    private String taskType;
    private String taskId;
    private String partitionKey;
}
