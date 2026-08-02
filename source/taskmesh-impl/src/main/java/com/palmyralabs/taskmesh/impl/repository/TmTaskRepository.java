package com.palmyralabs.taskmesh.impl.repository;
import com.palmyralabs.taskmesh.impl.entity.TaskId;
import com.palmyralabs.taskmesh.impl.entity.TaskStatus;
import com.palmyralabs.taskmesh.impl.entity.TmTaskEntity;

import jakarta.persistence.LockModeType;
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
 */
public interface TmTaskRepository extends JpaRepository<TmTaskEntity, TaskId> {

    /** Terminal snapshot — {@code attemptCount}/{@code maxAttempts} are copied from the hot row by the caller. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmTaskEntity t SET t.status = :status, "
            + "t.attemptCount = :attemptCount, t.maxAttempts = :maxAttempts, "
            + "t.reasonCode = :reasonCode, t.errorSummary = :errorSummary, t.updatedAt = :now "
            + "WHERE t.taskType = :taskType AND t.taskId = :taskId")
    int terminalUpdate(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("status") TaskStatus status, @Param("attemptCount") int attemptCount,
                        @Param("maxAttempts") int maxAttempts, @Param("reasonCode") String reasonCode,
                        @Param("errorSummary") String errorSummary, @Param("now") Instant now);

    /**
     * Pessimistic {@code SELECT … FOR UPDATE} by PK. Release takes this lock <b>first</b>, before
     * touching {@code tm_in_flight}, so release and {@code enqueue} acquire the two tables' locks
     * in the same order ({@code tm_task} → {@code tm_in_flight}) and can't deadlock.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT t FROM TmTaskEntity t WHERE t.taskType = :taskType AND t.taskId = :taskId")
    Optional<TmTaskEntity> lockByPk(@Param("taskType") String taskType, @Param("taskId") String taskId);

    // ---- Operational/reporting queries ----

    List<TmTaskEntity> findByTaskTypeAndStatus(String taskType, TaskStatus status);

    @Query("SELECT t.status, COUNT(t) FROM TmTaskEntity t WHERE t.taskType = :taskType GROUP BY t.status")
    List<Object[]> countByTaskType(@Param("taskType") String taskType);

    /** Operator-triggered rerun — unconditionally requeues regardless of current status. Pair with {@code TmInFlightRepository.resetForRerun}. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmTaskEntity t SET t.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.QUEUED, "
            + "t.updatedAt = :now WHERE t.taskType = :taskType AND t.taskId = :taskId")
    int forceRequeue(@Param("taskType") String taskType, @Param("taskId") String taskId, @Param("now") Instant now);
}
