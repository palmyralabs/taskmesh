package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.TaskLedger;
import com.palmyralabs.taskmesh.api.TaskRef;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.List;

/** Thin {@link TaskLedger} adapter over a shared {@link InMemoryTaskStore} (D22). */
@RequiredArgsConstructor
public class InMemoryTaskLedger implements TaskLedger {

    private final InMemoryTaskStore store;

    @Override
    public List<TaskRef> pullPending(String taskType, Instant now, int limit) {
        return store.findClaimable(taskType, now, limit);
    }

    @Override
    public void enqueue(TaskRef taskRef) {
        store.enqueue(taskRef);
    }

    @Override
    public void rerun(TaskRef taskRef) {
        store.rerun(taskRef);
    }

    @Override
    public int attemptCount(TaskRef taskRef) {
        return store.attemptCount(taskRef);
    }
}
