package com.palmyralabs.taskmesh.simple;
import com.palmyralabs.taskmesh.simple.entity.TaskId;
import com.palmyralabs.taskmesh.simple.entity.TaskStatus;
import com.palmyralabs.taskmesh.simple.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.simple.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.simple.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.simple.repository.TmTaskRepository;

import com.palmyralabs.taskmesh.api.AttemptCounters;
import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.DuplicateEnqueueException;
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
 *
 * <p><b>{@code ClaimStrategy}'s three methods open no transaction of their own</b> —
 * the caller supplies the boundary (the engine does, via {@code TransactionRunner}).
 * That is what lets a terminal transition and the consumer's {@code onTerminal}
 * callback commit or roll back as one unit, which a boundary opened down here could
 * never offer to code above it. {@code TaskLedger}'s {@code enqueue}/{@code rerun}
 * keep their own, since nothing needs to compose with them.
 *
 * <p><b>One lock order, everywhere: hot row, then cold.</b> Every write path here
 * touches {@code tm_in_flight} before {@code tm_task} — claim, release, rerun and
 * enqueue alike. It is forced by D33's claim-time termination, which already holds
 * the hot row by the time it discovers the task is terminal and so can never take
 * the cold row first; once one path is fixed that way, every other path must match
 * or they deadlock against each other (D34).
 *
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
        if (inFlightRepository.claimSingleNode(taskRef.taskType(), taskRef.taskId(),
                nodeInfoProvider.nodeId(), Instant.now()) != 1) {
            return ClaimOutcome.LOST;
        }
        return admitOrTerminate(taskRef);
    }

    /**
     * Budget is spent at claim time (D33). This tier has no lease, so the "node died
     * without releasing" case it guards against is instead "this node was killed and
     * {@link #recoverOwnClaims} handed the row back at startup" — same shape, same
     * need: without a claim-side check that task would be re-claimed forever.
     * Fenced by the claim this call just won; the terminal write runs in its own
     * transaction to keep the {@code tm_task} → {@code tm_in_flight} lock order.
     */
    private ClaimOutcome admitOrTerminate(TaskRef taskRef) {
        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .orElseThrow(() -> new IllegalStateException("tm_in_flight row missing for " + taskRef + " despite a won claim"));
        if (hot.getRetryCount() <= Math.min(hot.getMaxRetries(), retryPolicy.maxRetries())) {
            return ClaimOutcome.WON;
        }
        // Joins the caller's transaction — see MultiNodeTaskLedger#terminateExhausted. The claim
        // increment that tipped the budget over belongs to an execution that will never run, so
        // the snapshot records the last ordinal that actually did.
        taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), TaskStatus.FAILED,
                hot.getAttemptCount(), hot.getRetryCount() - 1, hot.getMaxRetries(),
                MultiNodeTaskLedger.RETRY_BUDGET_EXHAUSTED,
                "Retry budget exhausted with no reported outcome — the previous execution's node "
                        + "died without releasing", Instant.now());
        inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
        return ClaimOutcome.EXHAUSTED;
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
        return switch (request.disposition()) {
            case COMPLETED -> terminalRelease(taskRef, TaskStatus.COMPLETED, request);
            case FAILED_TERMINAL -> terminalRelease(taskRef, TaskStatus.FAILED, request);
            case RETRY -> retryRelease(taskRef, request);
            case YIELD -> yieldRelease(taskRef);
        };
    }

    private ReleaseOutcome terminalRelease(TaskRef taskRef, TaskStatus terminalStatus, ReleaseRequest request) {
        // Hot row FIRST, then cold — the one lock order every path in this class uses, including
        // D33's claim-time termination, which cannot take the cold row first because the claim
        // UPDATE has already taken the hot one. Locking tm_task first here instead (as this used
        // to) gives release and a claim-time termination of the same task opposite orders, and a
        // Postgres torture run deadlocked on it 40 times out of 40 — the exact shape a stale owner
        // releasing work another node has just reclaimed produces. Ordering against enqueue is not
        // at risk: enqueue writes nothing for a task that already exists, and a task that does not
        // exist has nothing to release.
        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElse(null);
        int deleted = inFlightRepository.deleteOwned(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId());
        if (deleted == 0 || hot == null) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }
        taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), terminalStatus,
                hot.getAttemptCount(), hot.getRetryCount(), hot.getMaxRetries(), request.reasonCode(), request.errorSummary(), Instant.now());
        return ReleaseOutcome.OK;
    }

    /**
     * Retryable failure — burns one unit of retry budget. The increment lands
     * <b>before</b> the exhaustion comparison (D33): post-increment,
     * {@code retry_count} names the execution that would run next, so
     * {@code max_retries = N} still allows N+1 executions per attempt, exactly
     * the budget the old pre-increment {@code attempt_count > max_attempts}
     * check gave. {@code attempt_count} is never touched here — only an
     * operator {@code rerun} moves the outer counter.
     */
    private ReleaseOutcome retryRelease(TaskRef taskRef, ReleaseRequest request) {
        Instant now = Instant.now();
        // Hot row first, then cold in the exhaustion branch — same single lock order as
        // terminalRelease, for the same reason.
        int fenced = inFlightRepository.resetUnclaimed(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now);
        if (fenced == 0) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }

        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .orElseThrow(() -> new IllegalStateException("tm_in_flight row missing for " + taskRef + " despite a fenced hot-row release"));
        int nextRetry = hot.getRetryCount() + 1;
        int effectiveMaxRetries = Math.min(hot.getMaxRetries(), retryPolicy.maxRetries());

        if (nextRetry > effectiveMaxRetries) {
            taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), TaskStatus.FAILED,
                    hot.getAttemptCount(), hot.getRetryCount(), hot.getMaxRetries(), request.reasonCode(), request.errorSummary(), now);
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            return ReleaseOutcome.EXHAUSTED;
        }

        // Within budget: reset to unclaimed (resetUnclaimed above) — no affinity, so node stays
        // cleared — plus the gate for the next execution. Neither counter is written here;
        // claim() spends the budget (D33), so bumping it again would double-bill.
        hot.setNextRetryAt(retryPolicy.nextRetryAt(nextRetry));
        hot.setUpdatedAt(now);
        inFlightRepository.save(hot);
        return ReleaseOutcome.OK;
    }

    /**
     * Cooperative hand-back (D33) — parks the row in {@code YIELDED} with no
     * backoff gate, so it is immediately claimable again once whatever caused
     * the yield clears, and the claim that picks it back up spends no budget
     * (that is what the distinct status is for). Fenced by {@code node_id}
     * like every other release.
     */
    private ReleaseOutcome yieldRelease(TaskRef taskRef) {
        int fenced = inFlightRepository.markYielded(taskRef.taskType(), taskRef.taskId(),
                nodeInfoProvider.nodeId(), Instant.now());
        return fenced == 1 ? ReleaseOutcome.OK : ReleaseOutcome.LOST_OWNERSHIP;
    }

    @Override
    public List<TaskRef> pullPending(String taskType, Instant now, int limit) {
        return inFlightRepository.findClaimableSingleNode(taskType, now, PageRequest.of(0, limit));
    }

    @Override
    public AttemptCounters counters(TaskRef taskRef) {
        return inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .map(hot -> new AttemptCounters(hot.getAttemptCount(), hot.getRetryCount()))
                // No hot row means a terminal outcome already deleted it — including claim-time
                // exhaustion (D33) — and the cold snapshot is then the truth.
                .orElseGet(() -> taskRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                        .map(cold -> new AttemptCounters(cold.getAttemptCount(), cold.getRetryCount()))
                        .orElse(AttemptCounters.NONE));
    }

    /**
     * Recreates the hot row and forces the cold row back to {@code QUEUED} in
     * one transaction; no-op if the task was never enqueued. Refuses
     * (throws) if a hot row already exists in any status — see
     * {@link #rerun} in {@code TaskLedger} for why. The new hot row's
     * {@code attempt_count} is the cold row's {@code attempt_count + 1} and
     * its {@code retry_count} restarts at <b>-1</b>, so its first claim bills it to 0 (D33), so a task that
     * exhausted its retries gets a full fresh budget. Safe only because
     * {@code tm_attempt_history} is keyed by <b>both</b> counters, so
     * {@code (1, 3)} and {@code (2, 0)} are distinct rows by construction.
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
            inFlightRepository.save(TmTaskEntities.newReservedRow(taskRef, nodeInfoProvider.nodeId(), existing.getAttemptCount() + 1, TmTaskEntities.NO_EXECUTION_YET));
            taskRepository.forceRequeue(taskRef.taskType(), taskRef.taskId(), Instant.now());
        });
    }

    @Override
    public void enqueue(TaskRef taskRef) {
        try {
            TaskId id = new TaskId(taskRef.taskType(), taskRef.taskId());
            if (taskRepository.findById(id).isEmpty()) {
                // Hot row before cold, the one lock order every path in this class uses. Nothing
                // could deadlock here either way — this branch only runs when the task does not
                // exist yet, so there is nothing for another node to be claiming or releasing —
                // but a single order with no exceptions is what keeps the invariant checkable.
                inFlightRepository.save(TmTaskEntities.newReservedRow(taskRef, nodeInfoProvider.nodeId(), 1, TmTaskEntities.NO_EXECUTION_YET));
                taskRepository.save(TmTaskEntities.newQueuedRow(taskRef));
            }
        } catch (DataIntegrityViolationException e) {
            throw new DuplicateEnqueueException(taskRef, e);
        }
    }
}
