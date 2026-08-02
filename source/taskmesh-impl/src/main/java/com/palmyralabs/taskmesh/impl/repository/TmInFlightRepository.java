package com.palmyralabs.taskmesh.impl.repository;
import com.palmyralabs.taskmesh.impl.entity.TaskId;
import com.palmyralabs.taskmesh.impl.entity.TaskStatus;
import com.palmyralabs.taskmesh.impl.entity.TmInFlightEntity;

import com.palmyralabs.taskmesh.api.TaskRef;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

/**
 * Hot-table repository (D-in_flight): everything here is claim-state churn,
 * kept off {@link TmTaskRepository}'s cold table entirely. Discovery queries
 * are driven FROM {@link TmInFlightEntity} (the small table) joined OUT to
 * {@link TmTaskEntity} for status/next_retry_at — not the other way round —
 * per explicit instruction, so the scan starts from the table sized by
 * current backlog/active count, not all-time history.
 */
public interface TmInFlightRepository extends JpaRepository<TmInFlightEntity, TaskId> {

    // ---- MultiNodeTaskLedger: 3-window (reclaim/affinity/timeout) claim (D27), now hot-table-scoped ----

    /**
     * Mirrors the old single-table {@code claimMultiNode} exactly, minus the
     * status/task_type filtering that now lives on {@link TmTaskEntity} —
     * expressed as an {@code EXISTS} subquery so this stays portable JPQL
     * (no native SQL / no Postgres-specific UPDATE...FROM needed). Must
     * mirror {@link #findClaimableMultiNode} exactly, or discovery and claim
     * drift (same palmyra76/workflow lesson as before the split).
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET "
            + "f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED, "
            + "f.nodeId = :nodeId, "
            + "f.claimedAt = CASE WHEN f.nodeId <> :nodeId OR f.claimedAt IS NULL OR f.claimedAt < :reclaimCutoff "
            + "                   THEN :now ELSE f.claimedAt END, "
            + "f.leaseExpiresAt = :leaseExpiresAt, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND EXISTS ("
            + "    SELECT 1 FROM TmTaskEntity t WHERE t.taskType = f.taskType AND t.taskId = f.taskId "
            + "    AND t.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.QUEUED"
            + ") AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED  AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.RELEASED AND f.nodeId = :nodeId AND f.updatedAt >= :affinityCutoff) "
            + "  OR (f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.RELEASED AND (f.updatedAt IS NULL OR f.updatedAt < :affinityCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED  AND f.updatedAt < :claimTimeoutCutoff)"
            + ")")
    int claimMultiNode(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("nodeId") String nodeId, @Param("now") Instant now,
                        @Param("reclaimCutoff") Instant reclaimCutoff, @Param("affinityCutoff") Instant affinityCutoff,
                        @Param("claimTimeoutCutoff") Instant claimTimeoutCutoff, @Param("leaseExpiresAt") Instant leaseExpiresAt);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.leaseExpiresAt = :leaseExpiresAt, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED AND f.leaseExpiresAt >= :now")
    int renewMultiNode(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("nodeId") String nodeId, @Param("now") Instant now, @Param("leaseExpiresAt") Instant leaseExpiresAt);

    /** Discovery for MultiNodeTaskLedger — driven FROM the hot table, joined OUT to the cold table. */
    @Query("SELECT new com.palmyralabs.taskmesh.api.TaskRef(f.taskType, f.taskId) FROM TmInFlightEntity f "
            + "JOIN TmTaskEntity t ON t.taskType = f.taskType AND t.taskId = f.taskId "
            + "WHERE f.taskType = :taskType AND t.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.QUEUED "
            + "AND (t.nextRetryAt IS NULL OR t.nextRetryAt <= :now) AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED  AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.RELEASED AND f.nodeId = :nodeId AND f.updatedAt >= :affinityCutoff) "
            + "  OR (f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.RELEASED AND (f.updatedAt IS NULL OR f.updatedAt < :affinityCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED  AND f.updatedAt < :claimTimeoutCutoff)"
            + ") ORDER BY t.createdAt ASC")
    List<TaskRef> findClaimableMultiNode(@Param("taskType") String taskType, @Param("nodeId") String nodeId,
                                          @Param("now") Instant now, @Param("affinityCutoff") Instant affinityCutoff,
                                          @Param("claimTimeoutCutoff") Instant claimTimeoutCutoff, Pageable pageable);

    /** Multi-node retryable release — keeps the row (RELEASED, node_id retained) for the affinity window. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.RELEASED, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED")
    int markReleased(@Param("taskType") String taskType, @Param("taskId") String taskId,
                      @Param("nodeId") String nodeId, @Param("now") Instant now);

    // ---- SimpleTaskLedger: single-node, no lease/affinity (infra-implementations.md Variant A) ----

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED, "
            + "f.nodeId = :nodeId, f.claimedAt = :now, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.status IS NULL AND EXISTS ("
            + "    SELECT 1 FROM TmTaskEntity t WHERE t.taskType = f.taskType AND t.taskId = f.taskId "
            + "    AND t.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.QUEUED)")
    int claimSingleNode(@Param("taskType") String taskType, @Param("taskId") String taskId,
                         @Param("nodeId") String nodeId, @Param("now") Instant now);

    @Query("SELECT new com.palmyralabs.taskmesh.api.TaskRef(f.taskType, f.taskId) FROM TmInFlightEntity f "
            + "JOIN TmTaskEntity t ON t.taskType = f.taskType AND t.taskId = f.taskId "
            + "WHERE f.taskType = :taskType AND f.status IS NULL "
            + "AND t.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.QUEUED "
            + "AND (t.nextRetryAt IS NULL OR t.nextRetryAt <= :now) ORDER BY t.createdAt ASC")
    List<TaskRef> findClaimableSingleNode(@Param("taskType") String taskType, @Param("now") Instant now, Pageable pageable);

    /** Single-node retryable release — no affinity concept, so unclaim immediately rather than keeping RELEASED. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = null, f.nodeId = null, f.claimedAt = null, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED")
    int resetUnclaimed(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("nodeId") String nodeId, @Param("now") Instant now);

    /** Startup recovery — this node is the only writer, so its own stale CLAIMED rows are definitionally abandoned. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = null, f.nodeId = null, f.claimedAt = null, f.updatedAt = :now "
            + "WHERE f.nodeId = :nodeId AND f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED")
    int recoverOwnClaims(@Param("nodeId") String nodeId, @Param("now") Instant now);

    // ---- Shared by both variants ----

    /** Terminal release (COMPLETED/FAILED, including exhaustion) — fenced delete; the hot row's job is done. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("DELETE FROM TmInFlightEntity f WHERE f.taskType = :taskType AND f.taskId = :taskId "
            + "AND f.nodeId = :nodeId AND f.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.CLAIMED")
    int deleteOwned(@Param("taskType") String taskType, @Param("taskId") String taskId, @Param("nodeId") String nodeId);

    /** Unconditional cleanup once exhaustion/terminal has already been fenced by the caller in the same transaction. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("DELETE FROM TmInFlightEntity f WHERE f.taskType = :taskType AND f.taskId = :taskId")
    void deleteByTaskTypeAndTaskId(@Param("taskType") String taskType, @Param("taskId") String taskId);

    /** Operator rerun — unconditionally resets to unclaimed regardless of current state. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = null, f.nodeId = null, f.claimedAt = null, "
            + "f.leaseExpiresAt = null, f.updatedAt = :now WHERE f.taskType = :taskType AND f.taskId = :taskId")
    int resetForRerun(@Param("taskType") String taskType, @Param("taskId") String taskId, @Param("now") Instant now);

    /** Backs the {@code /nodes} operational view — "what's each node claimed right now." */
    List<TmInFlightEntity> findByTaskTypeAndStatus(String taskType, TaskStatus status);

    /** CLAIMED/RELEASED counts — pair with {@code TmTaskRepository.countByTaskType} (QUEUED/COMPLETED/FAILED) for a full status rollup. */
    @Query("SELECT f.status, COUNT(f) FROM TmInFlightEntity f WHERE f.taskType = :taskType AND f.status IS NOT NULL GROUP BY f.status")
    List<Object[]> countByTaskType(@Param("taskType") String taskType);
}
