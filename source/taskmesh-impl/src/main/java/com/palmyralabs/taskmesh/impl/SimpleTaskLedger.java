package com.palmyralabs.taskmesh.impl;
import com.palmyralabs.taskmesh.impl.entity.TaskId;
import com.palmyralabs.taskmesh.impl.entity.TaskStatus;
import com.palmyralabs.taskmesh.impl.entity.TmInFlightEntity;
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

import java.time.Instant;
import java.util.List;

/**
 * Single-node {@code ClaimStrategy}/{@code TaskLedger} — no lease, no
 * affinity/reclaim/timeout (no cross-node race to arbitrate at all), per
 * {@code infra-implementations.md}'s Variant A. Retries reset the hot row to
 * unclaimed immediately rather than keeping a {@code RELEASED} affinity
 * window — there's no affinity concept here. Split across the cold
 * {@link TmTaskRepository} and hot {@link TmInFlightRepository} the same way
 * as {@link MultiNodeTaskLedger} — see {@link TmInFlightEntity}'s Javadoc.
 * Node identity comes from the given {@link NodeInfoProvider} (D17).
 */
@RequiredArgsConstructor
public class SimpleTaskLedger implements ClaimStrategy, TaskLedger {

    private final TmTaskRepository taskRepository;
    private final TmInFlightRepository inFlightRepository;
    private final TransactionTemplate transactionTemplate;
    private final NodeInfoProvider nodeInfoProvider;
    private final RetryPolicy retryPolicy;

    /**
     * Call once at startup — this node is the only writer, so any hot row it
     * left {@code CLAIMED} from a prior run is definitionally abandoned.
     */
    public void recoverOwnClaims() {
        transactionTemplate.executeWithoutResult(status -> inFlightRepository.recoverOwnClaims(nodeInfoProvider.nodeId(), Instant.now()));
    }

    @Override
    public ClaimOutcome claim(TaskRef taskRef) {
        Boolean won = transactionTemplate.execute(status ->
                inFlightRepository.claimSingleNode(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), Instant.now()) == 1);
        return Boolean.TRUE.equals(won) ? ClaimOutcome.WON : ClaimOutcome.LOST;
    }

    @Override
    public RenewOutcome renew(TaskRef taskRef) {
        // No lease to renew — just confirm this node still owns it.
        TaskId id = new TaskId(taskRef.taskType(), taskRef.taskId());
        boolean stillOwned = inFlightRepository.findById(id)
                .map(row -> row.getStatus() == TaskStatus.CLAIMED && nodeInfoProvider.nodeId().equals(row.getNodeId()))
                .orElse(false);
        return stillOwned ? RenewOutcome.RENEWED : RenewOutcome.LOST;
    }

    @Override
    public ReleaseOutcome release(TaskRef taskRef, ReleaseRequest request) {
        return transactionTemplate.execute(status -> switch (request.disposition()) {
            case COMPLETED -> terminalRelease(taskRef, TaskStatus.COMPLETED, request);
            case FAILED_TERMINAL -> terminalRelease(taskRef, TaskStatus.FAILED, request);
            case REQUEUE -> requeueRelease(taskRef, request);
        });
    }

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
        int fenced = inFlightRepository.resetUnclaimed(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now);
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
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            return ReleaseOutcome.EXHAUSTED;
        }

        task.setStatus(TaskStatus.QUEUED);
        task.setNextRetryAt(retryPolicy.nextRetryAt(nextAttempt));
        taskRepository.save(task);
        return ReleaseOutcome.OK;
    }

    @Override
    public List<TaskRef> pullPending(String taskType, Instant now, int limit) {
        return inFlightRepository.findClaimableSingleNode(taskType, now, PageRequest.of(0, limit));
    }

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
