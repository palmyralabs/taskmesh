package com.palmyralabs.taskmesh.impl;
import com.palmyralabs.taskmesh.impl.entity.TaskId;
import com.palmyralabs.taskmesh.impl.entity.TaskStatus;
import com.palmyralabs.taskmesh.impl.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.impl.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.impl.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.api.TaskLedger;
import com.palmyralabs.taskmesh.api.TaskRef;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Multi-node {@code ClaimStrategy}/{@code TaskLedger}, the 3-window
 * (reclaim/affinity/timeout) model (D27), split across the cold
 * {@link TmTaskRepository} (enqueue + terminal/requeue) and the hot
 * {@link TmInFlightRepository} (claim/renew/release churn) — see
 * {@link TmInFlightEntity}'s Javadoc for why. {@code reclaimWindow} governs
 * only the timestamp-refresh write-minimization inside the claim UPDATE,
 * never eligibility — the palmyra76/workflow "60s-150s dead zone" lesson.
 * Node identity comes from the given {@link NodeInfoProvider} (D17); build
 * one instance per node.
 */
@RequiredArgsConstructor
public class MultiNodeTaskLedger implements ClaimStrategy, TaskLedger {

    private final TmTaskRepository taskRepository;
    private final TmInFlightRepository inFlightRepository;
    private final TransactionTemplate transactionTemplate;
    private final NodeInfoProvider nodeInfoProvider;
    private final RetryPolicy retryPolicy;
    private final Duration leaseDuration;
    private final Duration reclaimWindow;
    private final Duration affinityWindow;
    private final Duration claimTimeout;

    @Override
    public ClaimOutcome claim(TaskRef taskRef) {
        Boolean won = transactionTemplate.execute(status -> {
            Instant now = Instant.now();
            int rows = inFlightRepository.claimMultiNode(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now,
                    now.minus(reclaimWindow), now.minus(affinityWindow), now.minus(claimTimeout), now.plus(leaseDuration));
            return rows == 1;
        });
        return Boolean.TRUE.equals(won) ? ClaimOutcome.WON : ClaimOutcome.LOST;
    }

    @Override
    public RenewOutcome renew(TaskRef taskRef) {
        Boolean renewed = transactionTemplate.execute(status -> {
            Instant now = Instant.now();
            int rows = inFlightRepository.renewMultiNode(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now, now.plus(leaseDuration));
            return rows == 1;
        });
        return Boolean.TRUE.equals(renewed) ? RenewOutcome.RENEWED : RenewOutcome.LOST;
    }

    @Override
    public ReleaseOutcome release(TaskRef taskRef, ReleaseRequest request) {
        return transactionTemplate.execute(status -> switch (request.disposition()) {
            case COMPLETED -> terminalRelease(taskRef, TaskStatus.COMPLETED, request);
            case FAILED_TERMINAL -> terminalRelease(taskRef, TaskStatus.FAILED, request);
            case REQUEUE -> requeueRelease(taskRef, request);
        });
    }

    /** Fenced via the hot-row delete — 0 rows means ownership was already lost, so the cold row is never touched. */
    private ReleaseOutcome terminalRelease(TaskRef taskRef, TaskStatus terminalStatus, ReleaseRequest request) {
        int deleted = inFlightRepository.deleteOwned(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId());
        if (deleted == 0) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }
        taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), terminalStatus,
                request.reasonCode(), request.errorSummary(), Instant.now());
        return ReleaseOutcome.OK;
    }

    private ReleaseOutcome requeueRelease(TaskRef taskRef, ReleaseRequest request) {
        Instant now = Instant.now();
        int fenced = inFlightRepository.markReleased(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now);
        if (fenced == 0) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }

        TmTaskEntity task = taskRepository.findForUpdate(taskRef.taskType(), taskRef.taskId())
                .orElseThrow(() -> new IllegalStateException("tm_task row missing for " + taskRef + " despite a fenced hot-row release"));
        int nextAttempt = task.getAttemptCount() + 1;
        int effectiveMaxAttempts = Math.min(task.getMaxAttempts(), retryPolicy.maxAttempts());

        task.setAttemptCount(nextAttempt);
        task.setReasonCode(request.reasonCode());
        task.setErrorSummary(request.errorSummary());
        task.setUpdatedAt(now);

        if (nextAttempt > effectiveMaxAttempts) {
            task.setStatus(TaskStatus.FAILED);
            task.setNextRetryAt(null);
            taskRepository.save(task);
            // Terminal — the hot row's just-set RELEASED state no longer means anything; drop it.
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            return ReleaseOutcome.EXHAUSTED;
        }

        // node_id NOT cleared by markReleased — kept as "last owner" for the affinity window (D27).
        task.setNextRetryAt(retryPolicy.nextRetryAt(nextAttempt));
        taskRepository.save(task);
        return ReleaseOutcome.OK;
    }

    @Override
    public List<TaskRef> pullPending(String taskType, Instant now, int limit) {
        return inFlightRepository.findClaimableMultiNode(taskType, nodeInfoProvider.nodeId(), now,
                now.minus(affinityWindow), now.minus(claimTimeout), PageRequest.of(0, limit));
    }

    /**
     * findById-then-save (both tables) is not atomic — two genuinely
     * concurrent callers for the same brand-new {@link TaskRef} can both
     * pass the empty check before either row exists, then collide on
     * {@code tm_task}'s primary key. {@link InFlightSet} doesn't protect
     * this path — {@code enqueue()} runs before that gate. Enqueue is
     * documented as idempotent, so losing that race is not an error: the
     * other caller's rows are exactly the outcome this call wanted too.
     */
    @Override
    public void enqueue(TaskRef taskRef) {
        try {
            transactionTemplate.executeWithoutResult(status -> {
                TaskId id = new TaskId(taskRef.taskType(), taskRef.taskId());
                if (taskRepository.findById(id).isEmpty()) {
                    taskRepository.save(TmTaskEntities.newQueuedRow(taskRef));
                    inFlightRepository.save(TmTaskEntities.newUnclaimedRow(taskRef));
                }
            });
        } catch (DataIntegrityViolationException e) {
            // Lost the race — another caller already inserted this TaskRef.
        }
    }
}
