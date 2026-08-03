package com.palmyralabs.taskmesh.max.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.SequenceGenerator;
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
@IdClass(TmHistoryEventId.class)
@Getter
@Setter
@NoArgsConstructor
public class TmHistoryEventEntity {

    /**
     * {@code IDENTITY} generation isn't supported inside an {@code @IdClass}
     * composite key (Hibernate: "Identity generation isn't supported for
     * composite ids") — {@code SEQUENCE} is, and doubles as the "id stays
     * globally unique across partitions" mechanism a physically-partitioned
     * table needs (D32) — an {@code IDENTITY}/{@code SERIAL} column isn't
     * reliably shared across Postgres partitions, an explicit sequence is.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "tm_history_event_seq")
    @SequenceGenerator(name = "tm_history_event_seq", sequenceName = "tm_history_event_id_seq", allocationSize = 1)
    private Long id;

    /** Caller-supplied (via the task's {@code TaskRef}) — required, part of the PK (D32). */
    @Id
    @Column(name = "partition_key", nullable = false, length = 255)
    private String partitionKey;

    @Column(name = "task_type", nullable = false, length = 128)
    private String taskType;

    @Column(name = "task_id", nullable = false, length = 255)
    private String taskId;

    @Column(name = "attempt_number", nullable = false)
    private int attemptNumber;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 32)
    private HistoryEventType eventType;

    @JdbcTypeCode(SqlTypes.LONGVARCHAR)
    @Column(name = "detail")
    private String detail;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;
}
