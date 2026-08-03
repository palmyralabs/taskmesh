package com.palmyralabs.taskmesh.max;
import com.palmyralabs.taskmesh.max.entity.PartitionedTaskId;
import com.palmyralabs.taskmesh.max.entity.TaskId;
import com.palmyralabs.taskmesh.max.entity.TaskStatus;
import com.palmyralabs.taskmesh.max.entity.TmInFlightEntity;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;

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
 * The one {@code ClaimStrategy}/{@code TaskLedger} for this tier (D32 — no
 * single-node variant here; partitioning is a multi-node/scale concept, and a
 * 100-crore-row migration isn't a single-node workload). {@code partitionKey}
 * is caller-supplied and required (D32) — {@link #enqueue}/{@link #rerun}
 * throw {@link IllegalArgumentException} if it's missing, rather than
 * defaulting to an unpartitioned/default bucket.
 *
 * <p><b>Caller contract</b>: {@code (taskType, taskId)} must map to exactly
 * one, stable {@code partitionKey} — the same task re-enqueued must always
 * carry the same partition key. This ledger doesn't enforce it: {@code
 * tm_task}'s PK includes {@code partitionKey} so two different values could
 * technically coexist there, but {@code tm_in_flight}'s PK deliberately does
 * NOT include it (never physically partitioned, D32) — a caller that
 * violates this contract gets an unsupported, inconsistent hot-row state,
 * not a clean error.
 *
 * <p>Query/claim/release refer to the hot {@link TmInFlightRepository}
 * exclusively — {@code tm_task} is written only at enqueue and, as a cold
 * snapshot, on the terminal transition (where the final
 * {@code attempt_count}/{@code max_attempts} are copied from the hot row
 * before it's deleted).
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
public class PartitionedTaskLedger implements ClaimStrategy, TaskLedger {

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
            int rows = inFlightRepository.claim(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now,
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
            int rows = inFlightRepository.renew(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now, now.plus(leaseDuration));
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
        taskRepository.lockByPk(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey());
        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId())).orElse(null);
        int deleted = inFlightRepository.deleteOwned(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId());
        if (deleted == 0 || hot == null) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }
        taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey(), terminalStatus,
                hot.getAttemptCount(), hot.getMaxAttempts(), request.reasonCode(), request.errorSummary(), Instant.now());
        return ReleaseOutcome.OK;
    }

    private ReleaseOutcome requeueRelease(TaskRef taskRef, ReleaseRequest request) {
        Instant now = Instant.now();
        // Lock the cold row first (same rationale as terminalRelease) — the exhaustion branch writes tm_task.
        taskRepository.lockByPk(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey());
        int fenced = inFlightRepository.markReleased(taskRef.taskType(), taskRef.taskId(), nodeInfoProvider.nodeId(), now);
        if (fenced == 0) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }

        TmInFlightEntity hot = inFlightRepository.findById(new TaskId(taskRef.taskType(), taskRef.taskId()))
                .orElseThrow(() -> new IllegalStateException("tm_in_flight row missing for " + taskRef + " despite a fenced hot-row release"));
        int nextAttempt = hot.getAttemptCount() + 1;
        int effectiveMaxAttempts = Math.min(hot.getMaxAttempts(), retryPolicy.maxAttempts());

        if (nextAttempt > effectiveMaxAttempts) {
            // Exhausted — snapshot the final counters onto the cold row, then drop the just-RELEASED hot row.
            taskRepository.terminalUpdate(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey(), TaskStatus.FAILED,
                    nextAttempt, hot.getMaxAttempts(), request.reasonCode(), request.errorSummary(), now);
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            return ReleaseOutcome.EXHAUSTED;
        }

        // Within budget — bump the counter and set the backoff gate on the hot row.
        // node_id retained (RELEASED, from markReleased) so the affinity window has a key.
        hot.setAttemptCount(nextAttempt);
        hot.setNextRetryAt(retryPolicy.nextRetryAt(nextAttempt));
        hot.setUpdatedAt(now);
        inFlightRepository.save(hot);
        return ReleaseOutcome.OK;
    }

    @Override
    public List<TaskRef> pullPending(String taskType, Instant now, int limit) {
        return inFlightRepository.findClaimable(taskType, nodeInfoProvider.nodeId(), now,
                now.minus(reservationWindow), now.minus(affinityWindow), now.minus(claimTimeout), PageRequest.of(0, limit));
    }

    /**
     * Recreates the hot claim row (a terminal outcome may have deleted it),
     * reserved to this node, and forces the cold row back to {@code QUEUED} —
     * all in one transaction. No-op if the task was never enqueued (keeps the
     * cold/hot pair invariant intact). Forcibly resets even an in-progress
     * task; the displaced worker's release then reports {@code LOST_OWNERSHIP}.
     *
     * <p>{@code partitionKey} must be supplied by the caller — it's required
     * to even form the lookup key ({@link PartitionedTaskId}), since it's part
     * of {@code tm_task}'s PK. No fallback cross-partition lookup (D32: caller
     * always supplies it, or fail loud).
     */
    @Override
    public void rerun(TaskRef taskRef) {
        if (taskRef.partitionKey() == null) {
            throw new IllegalArgumentException("partitionKey is required for taskmesh-max: " + taskRef);
        }
        transactionTemplate.executeWithoutResult(status -> {
            PartitionedTaskId id = new PartitionedTaskId(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey());
            if (taskRepository.findById(id).isEmpty()) {
                return;
            }
            inFlightRepository.deleteByTaskTypeAndTaskId(taskRef.taskType(), taskRef.taskId());
            inFlightRepository.save(TmTaskEntities.newReservedRow(taskRef, nodeInfoProvider.nodeId()));
            taskRepository.forceRequeue(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey(), Instant.now());
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
     * too.
     *
     * <p>Only writes the cold {@code tm_task} row — unlike taskmesh-simple/-pro,
     * this tier's hot-row creation is entirely the wired
     * {@code BacklogPromoter}'s concern (see {@code NoopBacklogPromoter}/
     * {@code JpaBacklogPromoter}), called by the engine right after this
     * returns.
     *
     * <p>{@code partitionKey} is required (D32) — thrown loudly here rather
     * than silently accepted, since a {@code NULL} partition column breaks
     * physical partitioning on {@code tm_task} and there's no default bucket.
     */
    @Override
    public void enqueue(TaskRef taskRef) {
        if (taskRef.partitionKey() == null) {
            throw new IllegalArgumentException("partitionKey is required for taskmesh-max: " + taskRef);
        }
        try {
            transactionTemplate.executeWithoutResult(status -> {
                PartitionedTaskId id = new PartitionedTaskId(taskRef.taskType(), taskRef.taskId(), taskRef.partitionKey());
                if (taskRepository.findById(id).isEmpty()) {
                    taskRepository.save(TmTaskEntities.newQueuedRow(taskRef));
                }
            });
        } catch (DataIntegrityViolationException e) {
            // Lost the race — another caller already inserted this TaskRef.
        }
    }
}
