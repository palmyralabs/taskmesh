package com.palmyralabs.taskmesh.simple.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "tm_history_event")
@Getter
@Setter
@NoArgsConstructor
public class TmHistoryEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "task_type", nullable = false, length = 128)
    private String taskType;

    @Column(name = "task_id", nullable = false, length = 255)
    private String taskId;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 32)
    private HistoryEventType eventType;

    @JdbcTypeCode(SqlTypes.LONGVARCHAR)
    @Column(name = "detail")
    private String detail;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;
}
