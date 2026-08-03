package com.palmyralabs.taskmesh.pro;
import com.palmyralabs.taskmesh.pro.entity.TaskId;
import com.palmyralabs.taskmesh.pro.entity.TaskStatus;
import com.palmyralabs.taskmesh.pro.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.pro.entity.TmTaskEntity;
import com.palmyralabs.taskmesh.pro.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.pro.repository.TmTaskRepository;

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
 * Multi-node {@code ClaimStrategy}/{@code TaskLedger}. Query/claim/release
 * refer to the hot {@link TmInFlightRepository} exclusively — {@code tm_task}
 * is written only at enqueue and, as a cold snapshot, on the terminal
 * transition (where the final {@code attempt_count}/{@code max_attempts} are
 * copied from the hot row before it's deleted).
 *
 * <p>Four windows govern eligibility, all keyed off the hot row:
 * <ul>
 *   <li>{@code reservationWindow} — a freshly enqueued {@code QUEUED} row is
 *       reserved to the enqueuing node; another node may steal it only once
 *       the row is older than this window (so a producer gets first crack at
 *       its own work, but a crashed/overloaded producer never strands it).</li>
 *   <li>{@code reclaimWindow} — governs only the {@code claimed_at} refresh
 *       inside the claim UPDATE (write-minimization), never eligibility.</li>
 *   <li>{@code affinityWindow} — a {@code RELEASED} row's last owner gets
 *       first refusal on the re-claim.</li>
 *   <li>{@code claimTimeout} — a {@code CLAIMED} row whose owner went silent
 *       is presumed dead and stealable.</li>
 * </ul>
 * Node identity comes from the given {@link NodeInfoProvider} (D17); build one
 * instance per node.
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
    private final Duration reservationWindow;

    @Override
    public ClaimOutcome claim(TaskRef taskRef) {
        Boolean won = transactionTemplate.execute(status -> {
            Instant now = Instant.now();
            int rows = inFlightRepository.claimMultiNode(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now,
                    now.minus(reclaimWindow), now.minus(reservationWindow), now.minus(affinityWindow), now.minus(claimTimeout),
                    now.plus(leaseDuration));
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

    /**
     * Fenced via the hot-row delete — 0 rows means ownership was already lost,
     * so the cold row is never touched. The hot row's final counters are read
     * first and copied onto the cold snapshot (D-terminal-snapshot).
     */
    private ReleaseOutcome terminalRelease(TaskRef taskRef, TaskStatus terminalStatus, ReleaseRequest request) {
        // Lock the cold row FIRST — same order as enqueue (tm_task -> tm_in_flight) — so release and a
        // concurrent enqueue of the same task can't deadlock on the two tables.
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
        int fenced = inFlightRepository.markReleased(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now);
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
            // Exhausted — snapshot the final counters onto the cold row, then drop the just-RELEASED hot row.
            taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), TaskStatus.FAILED,
                    hot.getAttemptCount(), hot.getMaxAttempts(), request.reasonCode(), request.errorSummary(), now);
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            return ReleaseOutcome.EXHAUSTED;
        }

        // Within budget — bump the counter to the next attempt's own number and set the backoff gate.
        // node_id retained (RELEASED, from markReleased) so the affinity window has a key.
        int nextAttempt = hot.getAttemptCount() + 1;
        hot.setAttemptCount(nextAttempt);
        hot.setNextRetryAt(retryPolicy.nextRetryAt(nextAttempt));
        hot.setUpdatedAt(now);
        inFlightRepository.save(hot);
        return ReleaseOutcome.OK;
    }

    @Override
    public List<TaskRef> pullPending(String taskType, Instant now, int limit) {
        return inFlightRepository.findClaimableMultiNode(taskType, nodeInfoProvider.nodeId(), now,
                now.minus(reservationWindow), now.minus(affinityWindow), now.minus(claimTimeout), PageRequest.of(0, limit));
    }

    @Override
    public int attemptCount(TaskRef taskRef) {
        return inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .map(TmInFlightEntity::getAttemptCount)
                .orElse(0);
    }

    /**
     * Recreates the hot claim row (a terminal outcome may have deleted it),
     * reserved to this node, and forces the cold row back to {@code QUEUED} —
     * all in one transaction. No-op if the task was never enqueued (keeps the
     * cold/hot pair invariant intact). Refuses (throws) if a hot row already
     * exists in any status — see {@link #rerun} in {@code TaskLedger} for why.
     *
     * <p>The new hot row's {@code attempt_count} is seeded from the cold
     * row's {@code attempt_count + 1} — cold holds the last attempt that
     * actually ran (1 for a never-attempted task's first rerun onward, or the
     * exhausted count from a prior terminal outcome), and the hot row always
     * holds the current one. Never reset to 0 — {@code tm_attempt_history} is
     * keyed by {@code (taskType, taskId, attemptNumber)}, so restarting the
     * counter would make the next attempt's history write collide with and
     * silently overwrite a pre-rerun row of the same number.
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
                                + "was deleted by a terminal outcome; a still-live task doesn't need it — if it's "
                                + "genuinely stuck, the ordinary claim/lease recovery path will pick it back up.");
            });
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            inFlightRepository.save(TmTaskEntities.newReservedRow(taskRef, nodeInfoProvider.nodeId(), existing.getAttemptCount() + 1));
            taskRepository.forceRequeue(taskRef.taskType(), taskRef.taskId(), Instant.now());
        });
    }

    /**
     * findById-then-save (both tables) is not atomic — two genuinely
     * concurrent callers for the same brand-new {@link TaskRef} can both
     * pass the empty check before either row exists, then collide on
     * {@code tm_task}'s primary key. {@link com.palmyralabs.taskmesh.engine.impl.InFlightSet}
     * doesn't protect this path — {@code enqueue()} runs before that gate.
     * Enqueue is documented as idempotent, so losing that race is not an
     * error: the other caller's rows are exactly the outcome this call wanted
     * too. The hot row is reserved to this node ({@code status = QUEUED}).
     */
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
