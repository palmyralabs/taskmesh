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

import java.time.Instant;

/**
 * The hot, tiny claim-state table (mirrors the toolkit's original
 * {@code in_flight} design, pulled forward per explicit instruction — all
 * claim/renew/release churn lands here, never on {@link TmTaskEntity}, so
 * that table's autovacuum cost stays proportional to current backlog/active
 * count, not all-time historical volume).
 *
 * <p>One row per {@code (task_type, task_id)} that has been enqueued and
 * isn't yet terminal — created alongside the {@link TmTaskEntity} row at
 * enqueue time and deleted entirely once the task reaches a terminal outcome
 * ({@code COMPLETED} or {@code FAILED} — including exhaustion). {@code status}
 * holds one of:
 * <ul>
 *   <li>{@code QUEUED} — freshly enqueued via {@code enqueue}/{@code execute},
 *       reserved to {@code node_id} (the enqueuing node); other nodes may
 *       steal it only after the reservation window — see {@link MultiNodeTaskLedger}.</li>
 *   <li>{@code null} — bulk/externally-populated, unreserved, claimable by any node.</li>
 *   <li>{@code CLAIMED} — actively owned by {@code node_id}.</li>
 *   <li>{@code RELEASED} — given back for retry, {@code node_id} kept as last owner.</li>
 * </ul>
 * {@code COMPLETED}/{@code FAILED} live exclusively on {@link TmTaskEntity}.
 *
 * <p>This table is the <b>sole</b> source of truth for query/claim/release —
 * the retry counters ({@code attempt_count}/{@code max_attempts}) and the
 * backoff gate ({@code next_retry_at}) live here too, so those operations
 * never read {@link TmTaskEntity} (which is written only at enqueue and, as a
 * cold snapshot, on the terminal transition).
 *
 * <p>{@code node_id} is <b>not</b> cleared on a retryable release from
 * {@link MultiNodeTaskLedger} — kept as "last owner" metadata (paired with
 * {@code status='RELEASED'}) so the affinity window has something to key on.
 * {@link SimpleTaskLedger} clears it immediately (no affinity concept).
 */
@Entity
@Table(name = "tm_in_flight")
@IdClass(TaskId.class)
@Getter
@Setter
@NoArgsConstructor
public class TmInFlightEntity {

    @Id
    @Column(name = "task_type", nullable = false, length = 128)
    private String taskType;

    @Id
    @Column(name = "task_id", nullable = false, length = 255)
    private String taskId;

    /** {@code QUEUED} (reserved to {@code nodeId}) / {@code null} (unreserved) / {@code CLAIMED} / {@code RELEASED}. */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 32)
    private TaskStatus status;

    @Column(name = "node_id")
    private String nodeId;

    /** Live retry counter (moved off {@link TmTaskEntity} — release reads/increments it here). */
    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    /** Per-task retry ceiling; the wired {@code RetryPolicy} is the effective gate (min of the two). */
    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts;

    /** Backoff gate for a requeued row — discovery skips it until this passes. {@code null} = immediately eligible. */
    @Column(name = "next_retry_at")
    private Instant nextRetryAt;

    @Column(name = "claimed_at")
    private Instant claimedAt;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
