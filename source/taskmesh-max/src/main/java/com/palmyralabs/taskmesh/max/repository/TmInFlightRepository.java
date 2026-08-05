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
 * {@code node_id}, the nested counters
 * ({@code attempt_count}/{@code retry_count}, bounded by {@code max_retries})
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
            // D33: every claim starts an execution and spends one unit of retry budget. The one
            // exception is resuming a YIELDED row, which is handing back work already paid for.
            // Billing here rather than in release() is what makes a lost lease cost anything:
            // the node that lost it is unfenced by construction and can never write. No sweeper
            // (which D27's visibility-based recovery deliberately avoids) and no extra round
            // trip. Postgres and H2 both evaluate SET expressions against the pre-UPDATE row.
            + "f.retryCount = f.retryCount + CASE WHEN f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.YIELDED "
            + "                                  THEN 0 ELSE 1 END, "
            + "f.claimedAt = CASE WHEN f.nodeId <> :nodeId OR f.claimedAt IS NULL OR f.claimedAt < :reclaimCutoff "
            + "                   THEN :now ELSE f.claimedAt END, "
            + "f.leaseExpiresAt = :leaseExpiresAt, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.YIELDED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.YIELDED   AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
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
            + "WHERE f.nextRetryAt <= :now AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.YIELDED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.YIELDED   AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED  AND f.nodeId = :nodeId AND f.updatedAt >= :affinityCutoff) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED  AND (f.updatedAt IS NULL OR f.updatedAt < :affinityCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED   AND f.updatedAt < :claimTimeoutCutoff)"
            + ") ORDER BY f.nextRetryAt ASC")
    List<TaskRef> findClaimable(@Param("nodeId") String nodeId,
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
    @Query("UPDATE TmInFlightEntity f SET f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.YIELDED, "
            + "f.leaseExpiresAt = null, f.nextRetryAt = :now, f.updatedAt = :now "
            + "WHERE f.taskType = :taskType AND f.taskId = :taskId AND f.nodeId = :nodeId "
            + "AND f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED")
    int markYielded(@Param("taskType") String taskType, @Param("taskId") String taskId,
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
            + "AND f.status IN (com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED, "
            + "com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED) GROUP BY f.status")
    List<Object[]> countByTaskType(@Param("taskType") String taskType);

    /**
     * Creates the hot rows for one promoted slice in <b>a single statement</b>, reading straight
     * from the cold rows this promoter just stamped.
     *
     * <p>Replaces a per-row loop that cost roughly {@code 3N + 4} statements: a {@code findById}
     * guard, plus a {@code save()} which — because {@code tm_in_flight} has an assigned composite
     * key and so never looks "new" to Spring Data — became a {@code merge()}, i.e. another SELECT
     * and then the INSERT. At a 500-row batch that was ~1,500 statements every promotion cycle,
     * running against the same connections and the same database as the drain it was feeding.
     * Measured cost: throttled promotion ran ~25% below eager at 64 workers/node.
     *
     * <p>{@code NOT EXISTS} does set-wise what the per-row guard did one row at a time, and for the
     * same reason: {@code save()} on an assigned key is an upsert, so writing over a live hot row
     * would silently reset a claimed task to unclaimed and lose its owner's claim. Kept even though
     * winning {@code stampPromoted} makes a collision nearly impossible — "nearly impossible" is
     * how the last one presented.
     *
     * <p>Native rather than JPQL: JPQL has no {@code INSERT ... SELECT}. The syntax here is plain
     * standard SQL, so it runs unchanged on both Postgres and the H2 the tests use.
     *
     * @param now the exact instant {@code stampPromoted} wrote, which identifies this call's slice
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "INSERT INTO tm_in_flight (task_type, task_id, partition_key, status, node_id, "
            + "attempt_count, retry_count, max_retries, next_retry_at, created_at, updated_at) "
            // attempt_count seeded from the cold row + 1 and retry_count at -1, exactly as
            // TmTaskEntities.newReservedRow does: a new attempt restarts the inner counter, and the
            // first claim bills it up to 0 (D33).
            + "SELECT t.task_type, t.task_id, t.partition_key, 'QUEUED', :nodeId, "
            + "t.attempt_count + 1, -1, :maxRetries, :now, :now, :now "
            + "FROM tm_task t "
            + "WHERE t.task_type = :taskType AND t.task_id IN :taskIds AND t.promoted_at = :now "
            + "AND NOT EXISTS (SELECT 1 FROM tm_in_flight f "
            + "                WHERE f.task_type = t.task_type AND f.task_id = t.task_id)",
            nativeQuery = true)
    int insertPromotedSlice(@Param("taskType") String taskType, @Param("taskIds") List<String> taskIds,
                             @Param("now") Instant now, @Param("nodeId") String nodeId,
                             @Param("maxRetries") int maxRetries);

    /**
     * How much claimable work this node has right now, across <b>every</b> task type — the
     * in-flight level {@link com.palmyralabs.taskmesh.max.JpaBacklogPromoter} regulates against.
     *
     * <p><b>Third copy of the eligibility block — keep it in exact sync with {@link #claim} and
     * {@link #findClaimable}.</b> It is {@link #findClaimable}'s {@code WHERE} verbatim, including
     * the {@code next_retry_at} backoff gate, with only the {@code taskType} filter dropped and
     * the projection replaced by a count. Anything that changes eligibility has to change here
     * too, or the promoter will size the window against a different notion of "claimable" than the
     * poller actually claims — which surfaces as workers idling in front of a full window, or a
     * window that never stops refilling.
     *
     * <p>Deliberately not scoped by {@code taskType}, unlike everything else here: the window is a
     * property of the node's capacity, not of any one task type. Scoping it per type would
     * multiply the configured window by however many types happen to be registered.
     *
     * <p>{@code QUEUED} rows are counted, unlike in {@link #countByTaskType} — there the exclusion
     * exists because that rollup is an operational view whose QUEUED count comes from the cold
     * side. Here a promoted-but-not-yet-claimed row is exactly what promotion just produced, and
     * ignoring it would make the promoter blind to its own output and refill every cycle.
     */
    @Query("SELECT COUNT(f) FROM TmInFlightEntity f "
            + "WHERE f.nextRetryAt <= :now AND ("
            + "     f.status IS NULL "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED    AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.YIELDED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.YIELDED   AND (f.nodeId IS NULL OR f.updatedAt < :reservationCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED   AND f.nodeId = :nodeId) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED  AND f.nodeId = :nodeId AND f.updatedAt >= :affinityCutoff) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.RELEASED  AND (f.updatedAt IS NULL OR f.updatedAt < :affinityCutoff)) "
            + "  OR (f.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.CLAIMED   AND f.updatedAt < :claimTimeoutCutoff)"
            + ")")
    long countClaimable(@Param("nodeId") String nodeId, @Param("now") Instant now,
                         @Param("reservationCutoff") Instant reservationCutoff,
                         @Param("affinityCutoff") Instant affinityCutoff,
                         @Param("claimTimeoutCutoff") Instant claimTimeoutCutoff);
}
