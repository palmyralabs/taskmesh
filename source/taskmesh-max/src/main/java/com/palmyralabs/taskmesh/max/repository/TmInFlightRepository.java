package com.palmyralabs.taskmesh.max.repository;
import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.entity.TaskStatus;
import com.palmyralabs.taskmesh.max.entity.TmInFlightEntity;

import com.palmyralabs.taskmesh.api.TaskRef;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

/**
 * Hot-table repository — the <b>sole</b> source of truth for query/claim/release
 * (D-in_flight, extended per explicit instruction: these operations refer to
 * this table and never {@code tm_task}). Everything they need lives here now —
 * {@code status} (including the {@code QUEUED} reservation state),
 * {@code node_id}, the retry counters ({@code attempt_count}/{@code max_attempts})
 * and the backoff gate ({@code next_retry_at}). {@code tm_task} is written only
 * at enqueue and, as a cold snapshot, on the terminal transition.
 *
 * <p><b>Eligibility must stay in exact sync</b> between {@link #claim} and
 * {@link #findClaimable} — the only difference is that discovery additionally
 * enforces the {@code next_retry_at} backoff gate (claim assumes its caller
 * already filtered through discovery). A drift between the two is the
 * recurring palmyra76/workflow bug; the four windows below
 * (reservation/reclaim/affinity/claim-timeout) each mean a distinct thing and
 * must not leak into one another.
 *
 * <p>{@code partition_key} is carried on this table (D32) but is <b>not</b>
 * part of its PK ({@link TaskId} — this table is never physically
 * partitioned) — {@link #findClaimable} projects it onto the returned
 * {@link TaskRef} so it propagates to {@code TaskContext}/history/incident
 * construction without a lookup back to {@code tm_task}.
 */
public interface TmInFlightRepository extends JpaRepository<TmInFlightEntity, TaskId> {

    /**
     * Atomic claim. Eligibility (the {@code WHERE ... AND (...)} block) mirrors
     * {@link #findClaimable} exactly. {@code reclaimCutoff} appears only in the
     * {@code claimed_at} refresh {@code CASE} (write-minimization), never in
     * eligibility — the "60s-150s dead zone" lesson.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET "
            + "f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED, "
            + "f.nodeId = :nodeId, "
            + "f.claimedAt = CASE WHEN f.nodeId <> :nodeId OR f.claimedAt IS NULL OR f.claimedAt < :reclaimCutoff "
            + "                   THEN :now ELSE f.claimedAt END, "
            + "f.leaseExpiresAt = :leaseExpiresAt, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED  AND f.nodeId = :nodeId AND f.updatedAt >= :affinityCutoff) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED  AND (f.updatedAt IS NULL OR f.updatedAt < :affinityCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED   AND f.updatedAt < :claimTimeoutCutoff)"
            + ")")
    int claim(@Param("taskType") String taskType, @Param("taskId") String taskId,
              @Param("nodeId") String nodeId, @Param("now") Instant now,
              @Param("reclaimCutoff") Instant reclaimCutoff, @Param("reservationCutoff") Instant reservationCutoff,
              @Param("affinityCutoff") Instant affinityCutoff, @Param("claimTimeoutCutoff") Instant claimTimeoutCutoff,
              @Param("leaseExpiresAt") Instant leaseExpiresAt);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.leaseExpiresAt = :leaseExpiresAt, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED AND f.leaseExpiresAt >= :now")
    int renew(@Param("taskType") String taskType, @Param("taskId") String taskId,
              @Param("nodeId") String nodeId, @Param("now") Instant now, @Param("leaseExpiresAt") Instant leaseExpiresAt);

    /**
     * Discovery — hot-table only (no {@code tm_task} join; a hot row exists
     * iff the task is non-terminal). Same eligibility as {@link #claim} plus
     * the {@code next_retry_at} backoff gate. Projects {@code partitionKey}
     * onto the returned {@link TaskRef}.
     */
    @Query("SELECT new com.palmyralabs.taskmesh.api.TaskRef(f.taskType, f.taskId, f.partitionKey) FROM TmInFlightEntity f "
            + "WHERE f.taskType = :taskType AND (f.nextRetryAt IS NULL OR f.nextRetryAt <= :now) AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED  AND f.nodeId = :nodeId AND f.updatedAt >= :affinityCutoff) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED  AND (f.updatedAt IS NULL OR f.updatedAt < :affinityCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED   AND f.updatedAt < :claimTimeoutCutoff)"
            + ") ORDER BY f.createdAt ASC")
    List<TaskRef> findClaimable(@Param("taskType") String taskType, @Param("nodeId") String nodeId,
                                 @Param("now") Instant now, @Param("reservationCutoff") Instant reservationCutoff,
                                 @Param("affinityCutoff") Instant affinityCutoff,
                                 @Param("claimTimeoutCutoff") Instant claimTimeoutCutoff, Pageable pageable);

    /** Retryable release — keeps the row (RELEASED, node_id retained) for the affinity window. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED")
    int markReleased(@Param("taskType") String taskType, @Param("taskId") String taskId,
                      @Param("nodeId") String nodeId, @Param("now") Instant now);

    /** Terminal release (COMPLETED/FAILED, including exhaustion) — fenced delete; the hot row's job is done. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("DELETE FROM TmInFlightEntity f WHERE f.taskType = :taskType AND f.taskId = :taskId "
            + "AND f.nodeId = :nodeId AND f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED")
    int deleteOwned(@Param("taskType") String taskType, @Param("taskId") String taskId, @Param("nodeId") String nodeId);

    /** Unconditional cleanup once exhaustion/terminal has already been fenced by the caller in the same transaction. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("DELETE FROM TmInFlightEntity f WHERE f.taskType = :taskType AND f.taskId = :taskId")
    void deleteByTaskTypeAndTaskId(@Param("taskType") String taskType, @Param("taskId") String taskId);

    /** Operator rerun — unconditionally resets to unclaimed (and clears retry progress) regardless of current state. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = null, f.nodeId = null, f.claimedAt = null, "
            + "f.leaseExpiresAt = null, f.attemptCount = 0, f.nextRetryAt = null, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId")
    int resetForRerun(@Param("taskType") String taskType, @Param("taskId") String taskId, @Param("now") Instant now);

    /** Backs the {@code /nodes} operational view — "what's each node claimed right now." */
    List<TmInFlightEntity> findByTaskTypeAndStatus(String taskType, TaskStatus status);

    /**
     * Active (CLAIMED/RELEASED) counts. QUEUED is deliberately excluded — it's
     * also on the cold {@code tm_task} row, and {@code TmTaskRepository.countByTaskType}
     * owns the QUEUED/COMPLETED/FAILED rollup; counting it here too would double it.
     */
    @Query("SELECT f.status, COUNT(f) FROM TmInFlightEntity f WHERE f.taskType = :taskType "
            + "AND f.status IN (com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED, "
            + "com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED) GROUP BY f.status")
    List<Object[]> countByTaskType(@Param("taskType") String taskType);
}
