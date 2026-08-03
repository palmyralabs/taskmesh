package com.palmyralabs.taskmesh.max.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "tm_incident")
@IdClass(TmIncidentId.class)
@Getter
@Setter
@NoArgsConstructor
public class TmIncidentEntity {

    /**
     * {@code IDENTITY} generation isn't supported inside an {@code @IdClass}
     * composite key — {@code SEQUENCE} is, and doubles as the "id stays
     * globally unique across partitions" mechanism a physically-partitioned
     * table needs (D32).
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "tm_incident_seq")
    @SequenceGenerator(name = "tm_incident_seq", sequenceName = "tm_incident_id_seq", allocationSize = 1)
    private Long id;

    /** Caller-supplied (via the task's {@code TaskRef}) — required, part of the PK (D32). */
    @Id
    @Column(name = "partition_key", nullable = false, length = 255)
    private String partitionKey;

    @Column(name = "task_type", nullable = false, length = 128)
    private String taskType;

    @Column(name = "task_id", nullable = false, length = 255)
    private String taskId;

    @Column(name = "reason_code", nullable = false, length = 64)
    private String reasonCode;

    @JdbcTypeCode(SqlTypes.LONGVARCHAR)
    @Column(name = "message")
    private String message;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    @Column(name = "resolved_by")
    private String resolvedBy;
}
