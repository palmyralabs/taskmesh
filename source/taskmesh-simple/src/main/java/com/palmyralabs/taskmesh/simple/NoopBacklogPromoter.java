package com.palmyralabs.taskmesh.simple;

import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.TaskRef;

import java.time.Instant;

/**
 * Default {@link BacklogPromoter} for this tier — {@link SimpleTaskLedger#enqueue}
 * and {@link MultiNodeTaskLedger#enqueue} already create the matching
 * {@code tm_in_flight} row eagerly, so there's nothing left for {@link #onEnqueue}
 * to do. {@link #promote} is a true no-op: nothing is ever left un-promoted at
 * this tier, so the engine's periodic promotion cycle is a cheap no-op call.
 */
public class NoopBacklogPromoter implements BacklogPromoter {

    @Override
    public void onEnqueue(TaskRef taskRef, String nodeId) {
        // no-op — the ledger's enqueue() already created the tm_in_flight row.
    }

    @Override
    public int promote(String taskType, Instant now, int limit) {
        return 0;
    }
}
