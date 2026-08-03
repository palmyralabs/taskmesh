package com.palmyralabs.taskmesh.simple.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * BATCH profile's coarse history shape (`AttemptHistoryWriter`,
 * infra-implementations.md's cleanser-style Variant A) — one row per
 * {@code (task_type, task_id, attempt_number)}, INSERTed on {@code STARTED}
 * and UPDATEd in place on the attempt's terminal outcome, instead of
 * {@link TmHistoryEventEntity}'s append-only per-event log. Fits BATCH's "a
 * handful of heavy, long-lived jobs" shape — full event granularity isn't
 * needed, and one row per attempt keeps the table tiny regardless of run count.
 */
@Entity
@Table(name = "tm_attempt_history")
@IdClass(TmAttemptHistoryId.class)
@Getter
@Setter
@NoArgsConstructor
public class TmAttemptHistoryEntity {

    @Id
    @Column(name = "task_type", nullable = false, length = 128)
    private String taskType;

    @Id
    @Column(name = "task_id", nullable = false, length = 255)
    private String taskId;

    @Id
    @Column(name = "attempt_number", nullable = false)
    private int attemptNumber;

    /** Last event type recorded for this attempt — STARTED until a terminal outcome overwrites it. */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private HistoryEventType status;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    @JdbcTypeCode(SqlTypes.LONGVARCHAR)
    @Column(name = "detail")
    private String detail;
}
