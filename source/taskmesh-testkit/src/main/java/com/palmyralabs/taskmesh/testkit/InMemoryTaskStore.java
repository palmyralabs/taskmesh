package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.AttemptCounters;
import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.api.TaskRef;

import java.time.Instant;
import java.util.Comparator;
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

    private enum Status { QUEUED, CLAIMED, YIELDED, COMPLETED, FAILED }

    private static class Row {
        TaskRef taskRef;
        Status status;
        String nodeId;
        int attemptCount;
        int retryCount;
        int maxRetries;
        Instant nextRetryAt;
    }

    /** A row that has never been claimed has no current execution; the first claim's +1 lands it on 0 (D33). */
    static final int NO_EXECUTION_YET = -1;

    private final Map<TaskRef, Row> rows = new ConcurrentHashMap<>();
    private final int defaultMaxRetries;

    public InMemoryTaskStore() {
        this(3);
    }

    public InMemoryTaskStore(int defaultMaxRetries) {
        this.defaultMaxRetries = defaultMaxRetries;
    }

    /**
     * {@code attemptCount} always holds the current attempt's own number
     * (never a "prior count" needing a {@code +1} from the caller) — 1 for a
     * freshly enqueued task — and {@code retryCount} the current execution's
     * 0-based ordinal within it, matching the production ledgers' hot-row
     * semantics (D33).
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
                row.retryCount = NO_EXECUTION_YET;
                row.maxRetries = defaultMaxRetries;
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
     * task doesn't need it. Bumps {@code attemptCount} by 1 and resets
     * {@code retryCount} to 0 (D33) — the re-run is a new outer attempt with
     * a full fresh retry budget, and the widened
     * {@code (attemptCount, retryCount)} history key keeps it from colliding
     * with anything already recorded.
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
            row.retryCount = NO_EXECUTION_YET;
            return row;
        });
    }

    /**
     * Discovery across <b>every</b> task type, oldest-visible first — mirrors the JPA tiers, where
     * the type filter moved off the query and into the engine's registry check. Ordering is by
     * {@code nextRetryAt} (the row's "not visible before" instant), so a task whose retry has just
     * come due interleaves correctly with fresh work instead of jumping the whole queue.
     */
    public synchronized List<TaskRef> findClaimable(Instant now, int limit) {
        List<Row> visible = new ArrayList<>();
        for (Row row : rows.values()) {
            if (row.status != Status.QUEUED && row.status != Status.YIELDED) {
                continue;
            }
            if (row.nextRetryAt != null && row.nextRetryAt.isAfter(now)) {
                continue;
            }
            visible.add(row);
        }
        // Nulls first: a row that has never been gated is visible from the moment it was created.
        visible.sort(Comparator.comparing(row -> row.nextRetryAt, Comparator.nullsFirst(Comparator.naturalOrder())));
        List<TaskRef> candidates = new ArrayList<>();
        for (Row row : visible) {
            if (candidates.size() >= limit) {
                break;
            }
            candidates.add(row.taskRef);
        }
        return candidates;
    }

    synchronized AttemptCounters counters(TaskRef taskRef) {
        Row row = rows.get(taskRef);
        return row != null ? new AttemptCounters(row.attemptCount, row.retryCount) : AttemptCounters.NONE;
    }

    /**
     * Mirrors the JPA ledgers' D33 rule: every claim starts an execution and spends
     * one unit of retry budget, except a claim that resumes a {@code YIELDED} row.
     * An execution that would be over budget is not admitted — the task is
     * terminated instead, which is the only way a task whose previous execution
     * died without releasing ever reaches a terminal state.
     */
    synchronized ClaimOutcome claim(TaskRef taskRef, String nodeId, RetryPolicy retryPolicy) {
        Row row = rows.get(taskRef);
        if (row == null || (row.status != Status.QUEUED && row.status != Status.YIELDED)) {
            return ClaimOutcome.LOST;
        }
        if (row.status != Status.YIELDED) {
            row.retryCount = row.retryCount + 1;
        }
        if (row.retryCount > Math.min(row.maxRetries, retryPolicy.maxRetries())) {
            row.retryCount = row.retryCount - 1;   // the execution that tipped it over never runs
            row.status = Status.FAILED;
            row.nodeId = null;
            return ClaimOutcome.EXHAUSTED;
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
            case RETRY -> {
                // The counter is claim()'s to move (D33). Release only decides, one step early,
                // whether a next execution exists at all: maxRetries=N allows N+1 per attempt.
                int nextRetry = row.retryCount + 1;
                if (nextRetry > Math.min(row.maxRetries, retryPolicy.maxRetries())) {
                    row.status = Status.FAILED;
                    row.nodeId = null;
                    yield ReleaseOutcome.EXHAUSTED;
                }
                row.status = Status.QUEUED;
                row.nodeId = null;
                row.nextRetryAt = retryPolicy.nextRetryAt(nextRetry);
                yield ReleaseOutcome.OK;
            }
            case YIELD -> {
                // Cooperative hand-back: no budget consumed, no backoff gate, and its own status so
                // the claim that picks it back up knows not to bill it.
                row.status = Status.YIELDED;
                row.nodeId = null;
                row.nextRetryAt = null;
                yield ReleaseOutcome.OK;
            }
        };
    }
}
