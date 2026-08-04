package com.palmyralabs.taskmesh.pro.entity;

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
 * {@code (task_type, task_id, attempt_count, retry_count)}, INSERTed on {@code STARTED}
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

    /**
     * Both counters are in the key (D33) — that is what makes {@code rerun}'s
     * {@code retry_count} reset safe: {@code (1, 3)} and {@code (2, 0)} are
     * distinct rows by construction, so a fresh attempt can never overwrite
     * pre-rerun history.
     */
    @Id
    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Id
    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    /** Last event type recorded for this attempt — STARTED until a terminal outcome overwrites it. */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private HistoryEventType status;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    @Column(name = "node_id", length = 255)
    private String nodeId;

    @JdbcTypeCode(SqlTypes.LONGVARCHAR)
    @Column(name = "detail")
    private String detail;
}
