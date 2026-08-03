package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.TaskRef;

import java.time.Instant;

/**
 * No-op {@link BacklogPromoter} for testkit — {@link InMemoryTaskStore#enqueue}
 * already makes a task immediately claimable, so there's no hot/cold split to
 * throttle here. Matches production's {@code NoopBacklogPromoter} default.
 */
public class InMemoryBacklogPromoter implements BacklogPromoter {

    @Override
    public void onEnqueue(TaskRef taskRef, String nodeId) {
        // no-op — InMemoryTaskStore.enqueue already made the task claimable.
    }

    @Override
    public int promote(String taskType, Instant now, int limit) {
        return 0;
    }
}
