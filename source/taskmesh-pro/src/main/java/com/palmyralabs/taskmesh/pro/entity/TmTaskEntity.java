package com.palmyralabs.taskmesh.pro.entity;

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
 * <p>Query/claim/release never read this table — it is written only at
 * enqueue ({@code QUEUED}) and on the terminal transition, where the final
 * {@code attempt_count}/{@code retry_count}/{@code max_retries} are <b>copied from the hot
 * {@link TmInFlightEntity} row</b> (the live counters live there now) as a
 * durable snapshot before that row is deleted. There is no {@code next_retry_at}
 * here anymore — the backoff gate moved to the hot row with the rest of the
 * claim-driving state.
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

    /** Final attempt number — snapshotted from the hot row on the terminal transition (0 until then). */
    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    /**
     * Retries burned within that final attempt — snapshotted alongside
     * {@code attempt_count} (D33). Without it, "how many retries did this burn
     * before it failed" is unanswerable once the hot row is deleted, which is
     * one of the signals the counter split exists to expose.
     */
    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    /** Snapshotted from the hot row on the terminal transition. */
    @Column(name = "max_retries", nullable = false)
    private int maxRetries;

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
