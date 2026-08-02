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
 * enqueue time ({@code status = null}, unclaimed), transitions to
 * {@code CLAIMED}/{@code RELEASED} as a ledger drives it, and is deleted
 * entirely once the task reaches a terminal outcome ({@code COMPLETED} or
 * {@code FAILED} — including exhaustion). {@code status} only ever holds
 * {@code CLAIMED} or {@code RELEASED} here; {@code QUEUED}/{@code COMPLETED}/
 * {@code FAILED} live exclusively on {@link TmTaskEntity}.
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

    /** {@code null} = staged/unclaimed. Only {@code CLAIMED}/{@code RELEASED} otherwise. */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 32)
    private TaskStatus status;

    @Column(name = "node_id")
    private String nodeId;

    @Column(name = "claimed_at")
    private Instant claimedAt;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
