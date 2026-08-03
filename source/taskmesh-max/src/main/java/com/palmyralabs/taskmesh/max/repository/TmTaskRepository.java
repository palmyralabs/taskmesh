package com.palmyralabs.taskmesh.max.repository;
import com.palmyralabs.taskmesh.max.entity.PartitionedTaskId;
import com.palmyralabs.taskmesh.max.entity.TaskStatus;
import com.palmyralabs.taskmesh.max.entity.TmTaskEntity;

import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Cold ledger repository — written only at enqueue and on the terminal
 * transition; never read by query/claim/release (all of which refer to the
 * hot {@link TmInFlightRepository} exclusively). The terminal write snapshots
 * the final {@code attempt_count}/{@code max_attempts} copied from the hot row
 * (the live counters live there now) before that row is deleted.
 *
 * <p>{@code partition_key} is part of this table's PK (D32) — every identity
 * lookup/update below takes it as a parameter, both because Postgres requires
 * it in the predicate for partition pruning and because it's part of the
 * entity's {@link PartitionedTaskId}.
 */
public interface TmTaskRepository extends JpaRepository<TmTaskEntity, PartitionedTaskId> {

    /** Terminal snapshot — {@code attemptCount}/{@code maxAttempts} are copied from the hot row by the caller. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmTaskEntity t SET t.status = :status, "
            + "t.attemptCount = :attemptCount, t.maxAttempts = :maxAttempts, "
            + "t.reasonCode = :reasonCode, t.errorSummary = :errorSummary, t.updatedAt = :now "
            + "WHERE t.taskType = :taskType AND t.taskId = :taskId AND t.partitionKey = :partitionKey")
    int terminalUpdate(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("partitionKey") String partitionKey, @Param("status") TaskStatus status,
                        @Param("attemptCount") int attemptCount, @Param("maxAttempts") int maxAttempts,
                        @Param("reasonCode") String reasonCode, @Param("errorSummary") String errorSummary,
                        @Param("now") Instant now);

    /**
     * Pessimistic {@code SELECT … FOR UPDATE} by PK. Release takes this lock <b>first</b>, before
     * touching {@code tm_in_flight}, so release and {@code enqueue} acquire the two tables' locks
     * in the same order ({@code tm_task} → {@code tm_in_flight}) and can't deadlock.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT t FROM TmTaskEntity t WHERE t.taskType = :taskType AND t.taskId = :taskId AND t.partitionKey = :partitionKey")
    Optional<TmTaskEntity> lockByPk(@Param("taskType") String taskType, @Param("taskId") String taskId,
                                     @Param("partitionKey") String partitionKey);

    // ---- Operational/reporting queries ----

    List<TmTaskEntity> findByTaskTypeAndStatus(String taskType, TaskStatus status);

    @Query("SELECT t.status, COUNT(t) FROM TmTaskEntity t WHERE t.taskType = :taskType GROUP BY t.status")
    List<Object[]> countByTaskType(@Param("taskType") String taskType);

    /** Operator-triggered rerun — unconditionally requeues regardless of current status. Pair with {@code TmInFlightRepository.resetForRerun}. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmTaskEntity t SET t.status = com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED, "
            + "t.updatedAt = :now WHERE t.taskType = :taskType AND t.taskId = :taskId AND t.partitionKey = :partitionKey")
    int forceRequeue(@Param("taskType") String taskType, @Param("taskId") String taskId,
                      @Param("partitionKey") String partitionKey, @Param("now") Instant now);

    // ---- JpaBacklogPromoter (throttled hot-row promotion) ----

    /**
     * Oldest still-backlogged rows first, bounded by {@code pageable}'s page
     * size — deliberately scoped by {@code taskType} only, not
     * {@code partitionKey}: promotion drains the whole backlog for a task
     * type across every partition, not one partition at a time.
     */
    @Query("SELECT t FROM TmTaskEntity t WHERE t.taskType = :taskType AND t.status = "
            + "com.palmyralabs.taskmesh.max.entity.TaskStatus.QUEUED AND t.promotedAt IS NULL "
            + "ORDER BY t.createdAt ASC")
    List<TmTaskEntity> findUnpromoted(@Param("taskType") String taskType, Pageable pageable);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmTaskEntity t SET t.promotedAt = :now "
            + "WHERE t.taskType = :taskType AND t.taskId IN :taskIds")
    int stampPromoted(@Param("taskType") String taskType, @Param("taskIds") List<String> taskIds, @Param("now") Instant now);
}
