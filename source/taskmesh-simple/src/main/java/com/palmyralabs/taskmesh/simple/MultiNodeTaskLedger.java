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

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Multi-node {@code ClaimStrategy}/{@code TaskLedger}. Query/claim/release
 * refer to the hot {@link TmInFlightRepository} exclusively — {@code tm_task}
 * is written only at enqueue and, as a cold snapshot, on the terminal
 * transition (where the final {@code attempt_count}/{@code retry_count}/
 * {@code max_retries} are copied from the hot row before it's deleted).
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

    /** Reason code for a task terminated at claim time because its budget was already spent (D33). */
    static final String RETRY_BUDGET_EXHAUSTED = "RETRY_BUDGET_EXHAUSTED";


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
        Instant now = Instant.now();
        int rows = inFlightRepository.claimMultiNode(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now,
                now.minus(reclaimWindow), now.minus(reservationWindow), now.minus(affinityWindow), now.minus(claimTimeout),
                now.plus(leaseDuration));
        if (rows != 1) {
            return ClaimOutcome.LOST;
        }
        return admitOrTerminate(taskRef);
    }

    /**
     * Budget is spent at claim time (D33), so the check lives here as well as in
     * {@link #retryRelease}: {@code retryRelease} catches the ordinary path (a node
     * that survives and reports its failure), and this catches the one it structurally
     * cannot — a node that died mid-execution and never got to release at all.
     *
     * <p>Deliberately runs <b>after</b> the claim UPDATE rather than as another
     * condition inside it. Two reasons: the eligibility block stays byte-identical
     * to {@code findClaimable}'s (the drift that block must never suffer), and the
     * terminal write is fenced by ownership this call has just acquired, so no
     * other node can be running the task while it happens.
     *
     * <p>The terminal write joins the caller's transaction, so the claim and the
     * termination commit as one unit — and so a caller can hang its own work (the
     * engine hangs {@code TaskProcessor.onTerminal}) off the same transaction.
     *
     * <p>It takes {@code tm_in_flight} first (from the claim UPDATE), then
     * {@code tm_task} — the single hot-then-cold lock order every write path in this
     * class follows. It has to: the claim already holds the hot row by the time
     * terminality is known, so this path cannot take the cold row first, and
     * therefore no other path may either. {@code release} used to lock {@code tm_task}
     * first and a Postgres torture run deadlocked against this method 40 times out of
     * 40 — the shape a stale owner releasing work another node has just reclaimed
     * produces. See {@code PostgresTortureTest}, which now asserts it does not.
     */
    private ClaimOutcome admitOrTerminate(TaskRef taskRef) {
        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .orElseThrow(() -> new IllegalStateException("tm_in_flight row missing for " + taskRef + " despite a won claim"));
        if (hot.getRetryCount() <= Math.min(hot.getMaxRetries(), retryPolicy.maxRetries())) {
            return ClaimOutcome.WON;
        }
        terminateExhausted(taskRef, hot);
        return ClaimOutcome.EXHAUSTED;
    }

    private void terminateExhausted(TaskRef taskRef, TmInFlightEntity hot) {
        // Snapshot the last ordinal that actually ran: the claim increment that tipped the budget
        // over belongs to an execution that is never going to happen, and the hot row carrying it
        // is about to be deleted anyway.
        taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), TaskStatus.FAILED,
                hot.getAttemptCount(), hot.getRetryCount() - 1, hot.getMaxRetries(),
                RETRY_BUDGET_EXHAUSTED,
                "Retry budget exhausted with no reported outcome — the previous execution's node "
                        + "lost its lease or died without releasing", Instant.now());
        inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
    }

    @Override
    public RenewOutcome renew(TaskRef taskRef) {
        Instant now = Instant.now();
        int rows = inFlightRepository.renewMultiNode(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now, now.plus(leaseDuration));
        return rows == 1 ? RenewOutcome.RENEWED : RenewOutcome.LOST;
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

    /**
     * Fenced via the hot-row delete — 0 rows means ownership was already lost,
     * so the cold row is never touched. The hot row's final counters are read
     * first and copied onto the cold snapshot (D-terminal-snapshot).
     */
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
     * Retryable failure. Does <b>not</b> move a counter — {@code claim} spends the
     * budget (D33) — but it does decide, one step early, whether the next execution
     * would be over it: {@code retry_count + 1 > max_retries} means there is no next
     * execution, so the task is terminated here rather than being claimed once more
     * only to be terminated then. {@code max_retries = N} allows N+1 executions per
     * attempt, exactly the budget the old {@code attempt_count > max_attempts} check
     * gave. {@code attempt_count} is never touched here — only an operator
     * {@code rerun} moves the outer counter.
     */
    private ReleaseOutcome retryRelease(TaskRef taskRef, ReleaseRequest request) {
        Instant now = Instant.now();
        // Hot row first, then cold in the exhaustion branch — same single lock order as
        // terminalRelease, for the same reason.
        int fenced = inFlightRepository.markReleased(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now);
        if (fenced == 0) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }

        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .orElseThrow(() -> new IllegalStateException("tm_in_flight row missing for " + taskRef + " despite a fenced hot-row release"));
        int nextRetry = hot.getRetryCount() + 1;
        int effectiveMaxRetries = Math.min(hot.getMaxRetries(), retryPolicy.maxRetries());

        if (nextRetry > effectiveMaxRetries) {
            // Exhausted — snapshot the final counters onto the cold row, then drop the just-RELEASED hot row.
            taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), TaskStatus.FAILED,
                    hot.getAttemptCount(), hot.getRetryCount(), hot.getMaxRetries(), request.reasonCode(), request.errorSummary(), now);
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            return ReleaseOutcome.EXHAUSTED;
        }

        // Within budget. Neither counter is written here — claim() spends the budget (D33), and
        // bumping it again would bill the same execution twice. All this sets is the gate for the
        // next execution, keyed off the ordinal claim is about to move the counter to, so prior
        // rerun history can't inflate a fresh attempt's backoff.
        // node_id retained (RELEASED, from markReleased) so the affinity window has a key.
        hot.setNextRetryAt(retryPolicy.nextRetryAt(nextRetry));
        hot.setUpdatedAt(now);
        inFlightRepository.save(hot);
        return ReleaseOutcome.OK;
    }

    /**
     * Cooperative hand-back while the lease is still held (D33) — the row
     * returns to {@code QUEUED} reserved to this node, the lease is dropped,
     * and neither the retry budget nor the backoff gate is touched, so it is
     * immediately claimable again once whatever caused the yield clears.
     * Fenced by {@code node_id} like every other release.
     */
    private ReleaseOutcome yieldRelease(TaskRef taskRef) {
        int fenced = inFlightRepository.markYielded(taskRef.taskType(), taskRef.taskId(),
                nodeInfoProvider.nodeId(), Instant.now());
        return fenced == 1 ? ReleaseOutcome.OK : ReleaseOutcome.LOST_OWNERSHIP;
    }

    @Override
    public List<TaskRef> pullPending(Instant now, int limit) {
        return inFlightRepository.findClaimableMultiNode(nodeInfoProvider.nodeId(), now,
                now.minus(reservationWindow), now.minus(affinityWindow), now.minus(claimTimeout), PageRequest.of(0, limit));
    }

    @Override
    public AttemptCounters counters(TaskRef taskRef) {
        return inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .map(hot -> new AttemptCounters(hot.getAttemptCount(), hot.getRetryCount()))
                // No hot row means the task just reached a terminal outcome — including the
                // claim-time exhaustion that deletes it (D33). The cold snapshot is then the
                // truth, and the caller still needs the pair to report what happened.
                .orElseGet(() -> taskRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                        .map(cold -> new AttemptCounters(cold.getAttemptCount(), cold.getRetryCount()))
                        .orElse(AttemptCounters.NONE));
    }

    /**
     * Recreates the hot claim row (a terminal outcome may have deleted it),
     * reserved to this node, and forces the cold row back to {@code QUEUED} —
     * all in one transaction. No-op if the task was never enqueued (keeps the
     * cold/hot pair invariant intact). Refuses (throws) if a hot row already
     * exists in any status — see {@link #rerun} in {@code TaskLedger} for why.
     *
     * <p>The new hot row's {@code attempt_count} is the cold row's
     * {@code attempt_count + 1} — cold holds the attempt that last actually
     * ran, and the hot row always holds the current one — while its
     * {@code retry_count} restarts at <b>-1</b>, so its first claim bills it to 0 (D33), so a task that
     * exhausted its retries gets a full fresh budget rather than
     * re-exhausting immediately. The reset is safe only because
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
                                + "was deleted by a terminal outcome; a still-live task doesn't need it — if it's "
                                + "genuinely stuck, the ordinary claim/lease recovery path will pick it back up.");
            });
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            inFlightRepository.save(TmTaskEntities.newReservedRow(taskRef, nodeInfoProvider.nodeId(), existing.getAttemptCount() + 1, TmTaskEntities.NO_EXECUTION_YET));
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
     *
     * <p><b>Opens no transaction of its own</b>, so the caller can put the
     * hot-row promotion that follows it in the same one (D34). That is why the
     * lost race is rethrown as {@link DuplicateEnqueueException} rather than
     * swallowed: swallowing only works while the failed insert is isolated —
     * inside a caller's transaction the violation has already marked it
     * rollback-only, and pretending otherwise poisons everything else in it.
     * Losing the race still isn't an error, so the engine catches this and
     * reports success; the winner's rows stand either way.
     */
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
