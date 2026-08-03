package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.api.TaskRef;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A shared, in-memory backing store standing in for the "shared claim/staging
 * table" design (D3) — one store, visible to every simulated node, so a
 * dead/slow node's abandoned work is naturally re-claimable by another
 * {@link InMemoryClaimStrategy} instance sharing this store. Construct one
 * store per test, and one {@link InMemoryClaimStrategy}/{@link InMemoryTaskLedger}
 * pair per simulated node against it.
 *
 * <p>Carries no payload (D23/D24) — payload resolution is entirely a
 * {@code PayloadProviderFactory}/{@code PayloadProvider} concern, external
 * to this store, matching how {@code TaskLedger}'s production implementations
 * never hold payload either.
 */
public class InMemoryTaskStore {

    private enum Status { QUEUED, CLAIMED, COMPLETED, FAILED }

    private static class Row {
        TaskRef taskRef;
        Status status;
        String nodeId;
        int attemptCount;
        int maxAttempts;
        Instant nextRetryAt;
    }

    private final Map<TaskRef, Row> rows = new ConcurrentHashMap<>();
    private final int defaultMaxAttempts;

    public InMemoryTaskStore() {
        this(3);
    }

    public InMemoryTaskStore(int defaultMaxAttempts) {
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    /**
     * {@code attemptCount} always holds the current/most-recently-run
     * attempt's own number (never a "prior count" needing a {@code +1} from
     * the caller) — 1 for a freshly enqueued task, matching the production
     * ledgers' hot-row semantics.
     */
    public void enqueue(TaskRef taskRef) {
        rows.compute(taskRef, (ref, existing) -> {
            Row row = existing != null ? existing : new Row();
            row.taskRef = taskRef;
            row.status = Status.QUEUED;
            row.nodeId = null;
            row.nextRetryAt = null;
            if (existing == null) {
                row.attemptCount = 1;
                row.maxAttempts = defaultMaxAttempts;
            }
            return row;
        });
    }

    /**
     * Operator rerun — resets a terminally-{@code COMPLETED}/{@code FAILED}
     * task to {@code QUEUED}; no-op if unknown. Throws if the task hasn't
     * reached a terminal outcome (still {@code QUEUED}/{@code CLAIMED}) —
     * matching the production ledgers, rerun only resurrects a task whose
     * "hot row" (here: terminal status) shows it's actually done; a still-live
     * task doesn't need it. Bumps {@code attemptCount} by 1 rather than
     * clearing it — the next {@code attemptNumber} must continue past the
     * pre-rerun attempts, not collide with one already recorded.
     */
    public void rerun(TaskRef taskRef) {
        rows.computeIfPresent(taskRef, (ref, row) -> {
            if (row.status == Status.QUEUED || row.status == Status.CLAIMED) {
                throw new IllegalStateException(
                        "Cannot rerun " + taskRef + " — status is " + row.status
                                + ", not yet terminal. rerun() only resurrects a task that has already "
                                + "COMPLETED/FAILED; a still-live task doesn't need it.");
            }
            row.status = Status.QUEUED;
            row.nodeId = null;
            row.nextRetryAt = null;
            row.attemptCount = row.attemptCount + 1;
            return row;
        });
    }

    public synchronized List<TaskRef> findClaimable(String taskType, Instant now, int limit) {
        List<TaskRef> candidates = new ArrayList<>();
        for (Row row : rows.values()) {
            if (candidates.size() >= limit) {
                break;
            }
            if (!row.taskRef.taskType().equals(taskType)) {
                continue;
            }
            if (row.status != Status.QUEUED) {
                continue;
            }
            if (row.nextRetryAt != null && row.nextRetryAt.isAfter(now)) {
                continue;
            }
            candidates.add(row.taskRef);
        }
        return candidates;
    }

    synchronized int attemptCount(TaskRef taskRef) {
        Row row = rows.get(taskRef);
        return row != null ? row.attemptCount : 0;
    }

    synchronized ClaimOutcome claim(TaskRef taskRef, String nodeId) {
        Row row = rows.get(taskRef);
        if (row == null || row.status != Status.QUEUED) {
            return ClaimOutcome.LOST;
        }
        row.status = Status.CLAIMED;
        row.nodeId = nodeId;
        return ClaimOutcome.WON;
    }

    synchronized RenewOutcome renew(TaskRef taskRef, String nodeId) {
        Row row = rows.get(taskRef);
        if (row == null || row.status != Status.CLAIMED || !nodeId.equals(row.nodeId)) {
            return RenewOutcome.LOST;
        }
        return RenewOutcome.RENEWED;
    }

    synchronized ReleaseOutcome release(TaskRef taskRef, String nodeId, ReleaseRequest request, RetryPolicy retryPolicy) {
        Row row = rows.get(taskRef);
        if (row == null || row.status != Status.CLAIMED || !nodeId.equals(row.nodeId)) {
            return ReleaseOutcome.LOST_OWNERSHIP;
        }
        return switch (request.disposition()) {
            case COMPLETED -> {
                row.status = Status.COMPLETED;
                row.nodeId = null;
                yield ReleaseOutcome.OK;
            }
            case FAILED_TERMINAL -> {
                row.status = Status.FAILED;
                row.nodeId = null;
                yield ReleaseOutcome.OK;
            }
            case REQUEUE -> {
                // Exhaustion is checked against the attempt that just ran (row.attemptCount — its own
                // number), not a "next candidate" — preserves maxAttempts=N allowing N+1 total attempts.
                if (row.attemptCount > Math.min(row.maxAttempts, retryPolicy.maxAttempts())) {
                    row.status = Status.FAILED;
                    row.nodeId = null;
                    yield ReleaseOutcome.EXHAUSTED;
                }
                int nextAttempt = row.attemptCount + 1;
                row.attemptCount = nextAttempt;
                row.status = Status.QUEUED;
                row.nodeId = null;
                row.nextRetryAt = retryPolicy.nextRetryAt(nextAttempt);
                yield ReleaseOutcome.OK;
            }
        };
    }
}
