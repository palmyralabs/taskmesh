package com.palmyralabs.taskmesh.pro.repository;
import com.palmyralabs.taskmesh.pro.entity.TaskId;
import com.palmyralabs.taskmesh.pro.entity.TaskStatus;
import com.palmyralabs.taskmesh.pro.entity.TmInFlightEntity;

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
 * {@code node_id}, the nested counters
 * ({@code attempt_count}/{@code retry_count}, bounded by {@code max_retries})
 * and the backoff gate ({@code next_retry_at}). {@code tm_task} is written only
 * at enqueue and, as a cold snapshot, on the terminal transition.
 *
 * <p><b>Eligibility must stay in exact sync</b> between {@link #claimMultiNode}
 * and {@link #findClaimableMultiNode} — the only difference is that discovery
 * additionally enforces the {@code next_retry_at} backoff gate (claim assumes
 * its caller already filtered through discovery). A drift between the two is
 * the recurring palmyra76/workflow bug; the four windows below
 * (reservation/reclaim/affinity/claim-timeout) each mean a distinct thing and
 * must not leak into one another.
 */
public interface TmInFlightRepository extends JpaRepository<TmInFlightEntity, TaskId> {

    // ---- MultiNodeTaskLedger: reservation + 3-window (reclaim/affinity/timeout) claim, hot-table-only ----

    /**
     * Atomic claim. Eligibility (the {@code WHERE ... AND (...)} block) mirrors
     * {@link #findClaimableMultiNode} exactly. {@code reclaimCutoff} appears
     * only in the {@code claimed_at} refresh {@code CASE} (write-minimization),
     * never in eligibility — the "60s-150s dead zone" lesson.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET "
            + "f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED, "
            + "f.nodeId = :nodeId, "
            // D33: every claim starts an execution and spends one unit of retry budget. The one
            // exception is resuming a YIELDED row, which is handing back work already paid for.
            // Billing here rather than in release() is what makes a lost lease cost anything:
            // the node that lost it is unfenced by construction and can never write. No sweeper
            // (which D27's visibility-based recovery deliberately avoids) and no extra round
            // trip. Postgres and H2 both evaluate SET expressions against the pre-UPDATE row.
            + "f.retryCount = f.retryCount + CASE WHEN f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED "
            + "                                  THEN 0 ELSE 1 END, "
            + "f.claimedAt = CASE WHEN f.nodeId <> :nodeId OR f.claimedAt IS NULL OR f.claimedAt < :reclaimCutoff "
            + "                   THEN :now ELSE f.claimedAt END, "
            + "f.leaseExpiresAt = :leaseExpiresAt, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.QUEUED    AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.QUEUED    AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED   AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.RELEASED  AND f.nodeId = :nodeId AND f.updatedAt >= :affinityCutoff) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.RELEASED  AND (f.updatedAt IS NULL OR f.updatedAt < :affinityCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED   AND f.updatedAt < :claimTimeoutCutoff)"
            + ")")
    int claimMultiNode(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("nodeId") String nodeId, @Param("now") Instant now,
                        @Param("reclaimCutoff") Instant reclaimCutoff, @Param("reservationCutoff") Instant reservationCutoff,
                        @Param("affinityCutoff") Instant affinityCutoff, @Param("claimTimeoutCutoff") Instant claimTimeoutCutoff,
                        @Param("leaseExpiresAt") Instant leaseExpiresAt);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.leaseExpiresAt = :leaseExpiresAt, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED AND f.leaseExpiresAt >= :now")
    int renewMultiNode(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("nodeId") String nodeId, @Param("now") Instant now, @Param("leaseExpiresAt") Instant leaseExpiresAt);

    /**
     * Discovery for MultiNodeTaskLedger — hot-table only (no {@code tm_task}
     * join; a hot row exists iff the task is non-terminal). Same eligibility as
     * {@link #claimMultiNode} plus the {@code next_retry_at} backoff gate.
     */
    @Query("SELECT new com.palmyralabs.taskmesh.api.TaskRef(f.taskType, f.taskId) FROM TmInFlightEntity f "
            + "WHERE f.nextRetryAt <= :now AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.QUEUED    AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.QUEUED    AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED   AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.RELEASED  AND f.nodeId = :nodeId AND f.updatedAt >= :affinityCutoff) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.RELEASED  AND (f.updatedAt IS NULL OR f.updatedAt < :affinityCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED   AND f.updatedAt < :claimTimeoutCutoff)"
            + ") ORDER BY f.nextRetryAt ASC")
    List<TaskRef> findClaimableMultiNode(@Param("nodeId") String nodeId,
                                          @Param("now") Instant now, @Param("reservationCutoff") Instant reservationCutoff,
                                          @Param("affinityCutoff") Instant affinityCutoff,
                                          @Param("claimTimeoutCutoff") Instant claimTimeoutCutoff, Pageable pageable);

    /** Multi-node retryable release — keeps the row (RELEASED, node_id retained) for the affinity window. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.RELEASED, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED")
    int markReleased(@Param("taskType") String taskType, @Param("taskId") String taskId,
                      @Param("nodeId") String nodeId, @Param("now") Instant now);

    /**
     * Cooperative yield (D33) — the owner hands unfinished work back while its
     * lease is still valid. Parks the row in {@code YIELDED} (its own status, so
     * the next claim knows not to bill it) reserved to the same node,
     * drops the lease, and opens the backoff gate (set to now, never null - the column is populated on every row so discovery can order by it): no retry budget is consumed
     * and nothing gates re-claim, so it is immediately claimable again once
     * whatever caused the yield clears. Fenced by {@code node_id} like every
     * other release.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED, "
            + "f.leaseExpiresAt = null, f.nextRetryAt = :now, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED")
    int markYielded(@Param("taskType") String taskType, @Param("taskId") String taskId,
                     @Param("nodeId") String nodeId, @Param("now") Instant now);

    // ---- SimpleTaskLedger: single-node, no lease/affinity/reservation (infra-implementations.md Variant A) ----

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED, "
            + "f.nodeId = :nodeId, f.claimedAt = :now, f.updatedAt = :now, "
            // Same D33 billing rule as claimMultiNode — see there.
            + "f.retryCount = f.retryCount + CASE WHEN f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED "
            + "                                  THEN 0 ELSE 1 END "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId "
            + "AND (f.status IS NULL OR f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.QUEUED "
            + "     OR f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED)")
    int claimSingleNode(@Param("taskType") String taskType, @Param("taskId") String taskId,
                         @Param("nodeId") String nodeId, @Param("now") Instant now);

    @Query("SELECT new com.palmyralabs.taskmesh.api.TaskRef(f.taskType, f.taskId) FROM TmInFlightEntity f "
            + "WHERE (f.status IS NULL OR f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.QUEUED "
            + "     OR f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.YIELDED) "
            + "AND f.nextRetryAt <= :now ORDER BY f.nextRetryAt ASC")
    List<TaskRef> findClaimableSingleNode(@Param("now") Instant now, Pageable pageable);

    /** Single-node retryable release — no affinity concept, so unclaim immediately rather than keeping RELEASED. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = null, f.nodeId = null, f.claimedAt = null, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED")
    int resetUnclaimed(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("nodeId") String nodeId, @Param("now") Instant now);

    /** Startup recovery — this node is the only writer, so its own stale CLAIMED rows are definitionally abandoned. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = null, f.nodeId = null, f.claimedAt = null, f.updatedAt = :now "
            + "WHERE f.nodeId = :nodeId AND f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED")
    int recoverOwnClaims(@Param("nodeId") String nodeId, @Param("now") Instant now);

    // ---- Shared by both variants ----

    /** Terminal release (COMPLETED/FAILED, including exhaustion) — fenced delete; the hot row's job is done. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("DELETE FROM TmInFlightEntity f WHERE f.taskType = :taskType AND f.taskId = :taskId "
            + "AND f.nodeId = :nodeId AND f.status = com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED")
    int deleteOwned(@Param("taskType") String taskType, @Param("taskId") String taskId, @Param("nodeId") String nodeId);

    /** Unconditional cleanup once exhaustion/terminal has already been fenced by the caller in the same transaction. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("DELETE FROM TmInFlightEntity f WHERE f.taskType = :taskType AND f.taskId = :taskId")
    void deleteByTaskTypeAndTaskId(@Param("taskType") String taskType, @Param("taskId") String taskId);

    /**
     * Operator rerun — unconditionally resets to unclaimed regardless of current
     * state, opening a new outer attempt with a full fresh retry budget (D33).
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmInFlightEntity f SET f.status = null, f.nodeId = null, f.claimedAt = null, "
            + "f.leaseExpiresAt = null, f.attemptCount = f.attemptCount + 1, f.retryCount = -1, "
            + "f.nextRetryAt = :now, f.updatedAt = :now "
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
            + "AND f.status IN (com.palmyralabs.taskmesh.pro.entity.TaskStatus.CLAIMED, "
            + "com.palmyralabs.taskmesh.pro.entity.TaskStatus.RELEASED) GROUP BY f.status")
    List<Object[]> countByTaskType(@Param("taskType") String taskType);
}
