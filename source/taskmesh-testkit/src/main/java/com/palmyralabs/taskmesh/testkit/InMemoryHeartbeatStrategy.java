package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.TaskRef;
import lombok.RequiredArgsConstructor;

/**
 * Unified-renewal style (D5): folds heartbeat straight into
 * {@link ClaimStrategy#renew}, no separate watchdog.
 */
@RequiredArgsConstructor
public class InMemoryHeartbeatStrategy implements HeartbeatStrategy {

    private final ClaimStrategy claimStrategy;

    @Override
    public RenewOutcome onCheckpoint(TaskRef taskRef) {
        return claimStrategy.renew(taskRef);
    }
}
