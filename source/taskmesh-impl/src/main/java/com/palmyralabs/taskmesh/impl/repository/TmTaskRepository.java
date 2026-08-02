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
 * Cold ledger repository — enqueue + terminal/requeue transitions only. All
 * claim-state churn and discovery queries moved to {@link TmInFlightRepository}
 * (D-in_flight); ownership fencing for a release now happens on the hot-table
 * op the caller runs in the same transaction, so these updates are keyed on
 * the row's identity alone.
 */
public interface TmTaskRepository extends JpaRepository<TmTaskEntity, TaskId> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmTaskEntity t SET t.status = :status, "
            + "t.reasonCode = :reasonCode, t.errorSummary = :errorSummary, t.updatedAt = :now "
            + "WHERE t.taskType = :taskType AND t.taskId = :taskId")
    int terminalUpdate(@Param("taskType") String taskType, @Param("taskId") String taskId,
                        @Param("status") TaskStatus status, @Param("reasonCode") String reasonCode,
                        @Param("errorSummary") String errorSummary, @Param("now") Instant now);

    /** REQUEUE's D16 exhaustion check — pessimistic-locked read, caller mutates+saves in the same transaction. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT t FROM TmTaskEntity t WHERE t.taskType = :taskType AND t.taskId = :taskId")
    Optional<TmTaskEntity> findForUpdate(@Param("taskType") String taskType, @Param("taskId") String taskId);

    // ---- Operational/reporting queries ----

    List<TmTaskEntity> findByTaskTypeAndStatus(String taskType, TaskStatus status);

    @Query("SELECT t.status, COUNT(t) FROM TmTaskEntity t WHERE t.taskType = :taskType GROUP BY t.status")
    List<Object[]> countByTaskType(@Param("taskType") String taskType);

    /** Operator-triggered rerun — unconditionally requeues regardless of current status. Pair with {@code TmInFlightRepository.resetForRerun}. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TmTaskEntity t SET t.status = com.palmyralabs.taskmesh.impl.entity.TaskStatus.QUEUED, "
            + "t.nextRetryAt = null, t.updatedAt = :now "
            + "WHERE t.taskType = :taskType AND t.taskId = :taskId")
    int forceRequeue(@Param("taskType") String taskType, @Param("taskId") String taskId, @Param("now") Instant now);
}
