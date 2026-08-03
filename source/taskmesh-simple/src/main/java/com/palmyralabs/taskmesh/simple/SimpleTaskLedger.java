package com.palmyralabs.taskmesh.simple;
import com.palmyralabs.taskmesh.simple.entity.TaskId;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.simple.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;

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
 * affinity/reclaim/timeout/reservation (no cross-node race to arbitrate at
 * all), per {@code infra-implementations.md}'s Variant A. Retries reset the
 * hot row to unclaimed immediately rather than keeping a {@code RELEASED}
 * affinity window. Like {@link MultiNodeTaskLedger}, query/claim/release refer
 * to the hot {@link TmInFlightRepository} exclusively; {@code tm_task} is
 * written only at enqueue and (as a snapshot) on the terminal transition.
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
        // Lock the cold row FIRST — same order as enqueue (tm_task -> tm_in_flight) — to avoid a deadlock
        // with a concurrent enqueue of the same task.
        taskRepository.lockByPk(taskRef.taskType(), taskRef.taskId());
        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElse(null);
        int deleted = inFlightRepository.deleteOwned(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId());
        if (deleted == 0 || hot == null) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }
        taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), terminalStatus,
                hot.getAttemptCount(), hot.getMaxAttempts(), request.reasonCode(), request.errorSummary(), Instant.now());
        return ReleaseOutcome.OK;
    }

    private ReleaseOutcome requeueRelease(TaskRef taskRef, ReleaseRequest request) {
        Instant now = Instant.now();
        // Lock the cold row first (same rationale as terminalRelease) — the exhaustion branch writes tm_task.
        taskRepository.lockByPk(taskRef.taskType(), taskRef.taskId());
        int fenced = inFlightRepository.resetUnclaimed(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now);
        if (fenced == 0) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }

        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .orElseThrow(() -> new IllegalStateException("tm_in_flight row missing for " + taskRef + " despite a fenced hot-row release"));
        int effectiveMaxAttempts = Math.min(hot.getMaxAttempts(), retryPolicy.maxAttempts());

        // Exhaustion is checked against the attempt that just ran (hot.getAttemptCount() — its own number,
        // set at claim time), not a "next candidate" — this preserves the historical retry-budget behavior
        // (maxAttempts=N allows N+1 total attempts) independent of how attempt_count is now numbered.
        if (hot.getAttemptCount() > effectiveMaxAttempts) {
            taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), TaskStatus.FAILED,
                    hot.getAttemptCount(), hot.getMaxAttempts(), request.reasonCode(), request.errorSummary(), now);
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            return ReleaseOutcome.EXHAUSTED;
        }

        // Reset to unclaimed (resetUnclaimed above) plus the bumped counter and backoff gate — no affinity, so node stays cleared.
        int nextAttempt = hot.getAttemptCount() + 1;
        hot.setAttemptCount(nextAttempt);
        hot.setNextRetryAt(retryPolicy.nextRetryAt(nextAttempt));
        hot.setUpdatedAt(now);
        inFlightRepository.save(hot);
        return ReleaseOutcome.OK;
    }

    @Override
    public List<TaskRef> pullPending(String taskType, Instant now, int limit) {
        return inFlightRepository.findClaimableSingleNode(taskType, now, PageRequest.of(0, limit));
    }

    @Override
    public int attemptCount(TaskRef taskRef) {
        return inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .map(TmInFlightEntity::getAttemptCount)
                .orElse(0);
    }

    /**
     * Recreates the hot row and forces the cold row back to {@code QUEUED} in
     * one transaction; no-op if the task was never enqueued. Refuses
     * (throws) if a hot row already exists in any status — see
     * {@link #rerun} in {@code TaskLedger} for why. The new hot row's
     * {@code attempt_count} is seeded from the cold row's
     * {@code attempt_count + 1} rather than resetting to 0 — {@code
     * tm_attempt_history} is keyed by {@code (taskType, taskId,
     * attemptNumber)}, so restarting the counter would make the next
     * attempt's history write collide with and silently overwrite a
     * pre-rerun row of the same number.
     */
    @Override
    public void rerun(TaskRef taskRef) {
        transactionTemplate.executeWithoutResult(status -> {
            TaskId id = new TaskId(taskRef.taskType(), taskRef.taskId());
            TmTaskEntity existing = taskRepository.findById(id).orElse(null);
            if (existing == null) {
                return;
            }
            inFlightRepository.findById(id).ifPresent(hotRow -> {
                throw new IllegalStateException(
                        "Cannot rerun " + taskRef + " — a hot row already exists (status " + hotRow.getStatus()
                                + ", node '" + hotRow.getNodeId() + "'). rerun() only resurrects a task whose hot row "
                                + "was deleted by a terminal outcome; this tier has no lease, so a still-live task "
                                + "recovers only via recoverOwnClaims() on that node's next restart.");
            });
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            inFlightRepository.save(TmTaskEntities.newReservedRow(taskRef, nodeInfoProvider.nodeId(), existing.getAttemptCount() + 1));
            taskRepository.forceRequeue(taskRef.taskType(), taskRef.taskId(), Instant.now());
        });
    }

    @Override
    public void enqueue(TaskRef taskRef) {
        try {
            transactionTemplate.executeWithoutResult(status -> {
                TaskId id = new TaskId(taskRef.taskType(), taskRef.taskId());
                if (taskRepository.findById(id).isEmpty()) {
                    taskRepository.save(TmTaskEntities.newQueuedRow(taskRef));
                    inFlightRepository.save(TmTaskEntities.newReservedRow(taskRef, nodeInfoProvider.nodeId(), 1));
                }
            });
        } catch (DataIntegrityViolationException e) {
            // Lost the race — another caller already inserted this TaskRef.
        }
    }
}
