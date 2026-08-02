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

    public void enqueue(TaskRef taskRef) {
        rows.compute(taskRef, (ref, existing) -> {
            Row row = existing != null ? existing : new Row();
            row.taskRef = taskRef;
            row.status = Status.QUEUED;
            row.nodeId = null;
            row.nextRetryAt = null;
            if (existing == null) {
                row.attemptCount = 0;
                row.maxAttempts = defaultMaxAttempts;
            }
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
                int nextAttempt = row.attemptCount + 1;
                if (nextAttempt > Math.min(row.maxAttempts, retryPolicy.maxAttempts())) {
                    row.status = Status.FAILED;
                    row.nodeId = null;
                    yield ReleaseOutcome.EXHAUSTED;
                }
                row.attemptCount = nextAttempt;
                row.status = Status.QUEUED;
                row.nodeId = null;
                row.nextRetryAt = retryPolicy.nextRetryAt(nextAttempt);
                yield ReleaseOutcome.OK;
            }
        };
    }
}
