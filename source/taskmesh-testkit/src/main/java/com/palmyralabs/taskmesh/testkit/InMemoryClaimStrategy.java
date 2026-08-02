package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.ClaimOutcome;
import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.ReleaseOutcome;
import com.palmyralabs.taskmesh.api.ReleaseRequest;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.api.TaskRef;
import lombok.RequiredArgsConstructor;

/**
 * Same WON/LOST/LOST_OWNERSHIP semantics as a real lease-based ClaimStrategy,
 * backed by an in-memory {@link InMemoryTaskStore} instead of Postgres.
 * Node identity comes from the given {@link NodeInfoProvider} (D17) — build
 * one instance per simulated node, sharing one {@link InMemoryTaskStore},
 * to unit-test multi-node claim races.
 */
@RequiredArgsConstructor
public class InMemoryClaimStrategy implements ClaimStrategy {

    private final InMemoryTaskStore store;
    private final NodeInfoProvider nodeInfoProvider;
    private final RetryPolicy retryPolicy;

    @Override
    public ClaimOutcome claim(TaskRef taskRef) {
        return store.claim(taskRef, nodeInfoProvider.nodeId());
    }

    @Override
    public RenewOutcome renew(TaskRef taskRef) {
        return store.renew(taskRef, nodeInfoProvider.nodeId());
    }

    @Override
    public ReleaseOutcome release(TaskRef taskRef, ReleaseRequest request) {
        return store.release(taskRef, nodeInfoProvider.nodeId(), request, retryPolicy);
    }
}
