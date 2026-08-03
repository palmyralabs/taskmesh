package com.palmyralabs.taskmesh.simple.entity;

import com.palmyralabs.taskmesh.api.ActorType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "tm_audit_event")
@Getter
@Setter
@NoArgsConstructor
public class TmAuditEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "task_type", length = 128)
    private String taskType;

    @Column(name = "task_id", length = 255)
    private String taskId;

    @Enumerated(EnumType.STRING)
    @Column(name = "actor_type", nullable = false, length = 32)
    private ActorType actorType;

    @Column(name = "performed_by")
    private String performedBy;

    @JdbcTypeCode(SqlTypes.LONGVARCHAR)
    @Column(name = "message")
    private String message;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;
}
