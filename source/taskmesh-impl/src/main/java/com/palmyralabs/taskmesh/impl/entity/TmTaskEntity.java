package com.palmyralabs.taskmesh.impl.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * The cold ledger table — one row per task for its entire lifetime,
 * touched only at enqueue and on terminal/requeue transitions. All claim
 * churn (claim/renew/release-in-flight) lives on {@link TmInFlightEntity}
 * instead, so this table's autovacuum cost stays proportional to write
 * volume (enqueue + terminal), not to lease-renewal frequency — the split
 * mirrors the toolkit's original {@code migration_task}/{@code in_flight}
 * design, pulled forward per explicit instruction (a portable-JPA version
 * of it, via JPQL cross-entity joins/subqueries rather than Postgres-only
 * {@code UPDATE...FROM}/fillfactor tuning).
 *
 * <p>{@code status} only ever holds {@code QUEUED}, {@code COMPLETED}, or
 * {@code FAILED} here — {@code CLAIMED}/{@code RELEASED} live exclusively
 * on {@link TmInFlightEntity}.
 *
 * <p>Carries no payload column (D23/D24) — payload resolution is entirely
 * the consuming project's own {@code PayloadProviderFactory}/{@code PayloadProvider}
 * concern, never taskmesh's to store.
 */
@Entity
@Table(name = "tm_task")
@IdClass(TaskId.class)
@Getter
@Setter
@NoArgsConstructor
public class TmTaskEntity {

    @Id
    @Column(name = "task_type", nullable = false, length = 128)
    private String taskType;

    @Id
    @Column(name = "task_id", nullable = false, length = 255)
    private String taskId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private TaskStatus status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts;

    @Column(name = "next_retry_at")
    private Instant nextRetryAt;

    @Column(name = "reason_code")
    private String reasonCode;

    @JdbcTypeCode(SqlTypes.LONGVARCHAR)
    @Column(name = "error_summary")
    private String errorSummary;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
