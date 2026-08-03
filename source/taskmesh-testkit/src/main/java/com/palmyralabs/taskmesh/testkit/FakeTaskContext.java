package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.ErrorLogger;
import com.palmyralabs.taskmesh.api.ProgressReporter;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskRef;

/**
 * Hand-built {@link TaskContext} for unit-testing a {@code TaskProcessor}
 * (typically via {@code TypedTaskProcessor}) without a database, engine, or
 * Spring context. Ownership/pause/shutdown flags are mutable so a test can
 * simulate a lease loss or a pause request landing mid-{@code process()}.
 */
public class FakeTaskContext implements TaskContext {

    private final TaskRef taskRef;
    private final int attemptNumber;
    private final Object payload;
    private final InMemoryProgressReporter progress = new InMemoryProgressReporter();
    private final InMemoryErrorLogger errorLogger = new InMemoryErrorLogger();
    private final Runnable onCheckpoint;

    private volatile boolean ownershipValid = true;
    private volatile boolean pauseRequested = false;
    private volatile boolean shutdownRequested = false;
    private int checkpointCount = 0;

    private FakeTaskContext(Builder builder) {
        this.taskRef = builder.taskRef;
        this.attemptNumber = builder.attemptNumber;
        this.payload = builder.payload;
        this.onCheckpoint = builder.onCheckpoint;
    }

    public static Builder builder(TaskRef taskRef, Object payload) {
        return new Builder(taskRef, payload);
    }

    @Override
    public TaskRef taskRef() {
        return taskRef;
    }

    @Override
    public int attemptNumber() {
        return attemptNumber;
    }

    @Override
    public Object payload() {
        return payload;
    }

    @Override
    public void checkpoint() {
        checkpointCount++;
        if (onCheckpoint != null) {
            onCheckpoint.run();
        }
    }

    @Override
    public boolean isOwnershipValid() {
        return ownershipValid;
    }

    @Override
    public boolean isPauseRequested() {
        return pauseRequested;
    }

    @Override
    public boolean isShutdownRequested() {
        return shutdownRequested;
    }

    @Override
    public ProgressReporter progress() {
        return progress;
    }

    @Override
    public ErrorLogger errorLogger() {
        return errorLogger;
    }

    // --- test-side controls, not part of TaskContext ---

    public void loseOwnership() {
        ownershipValid = false;
    }

    public void requestPause() {
        pauseRequested = true;
    }

    public void requestShutdown() {
        shutdownRequested = true;
    }

    public int checkpointCount() {
        return checkpointCount;
    }

    public InMemoryProgressReporter progressReporter() {
        return progress;
    }

    public InMemoryErrorLogger errorLoggerRecorder() {
        return errorLogger;
    }

    public static class Builder {
        private final TaskRef taskRef;
        private final Object payload;
        private int attemptNumber = 1;
        private Runnable onCheckpoint;

        private Builder(TaskRef taskRef, Object payload) {
            this.taskRef = taskRef;
            this.payload = payload;
        }

        /** Defaults to 1 (a task's first attempt) if not set. */
        public Builder attemptNumber(int attemptNumber) {
            this.attemptNumber = attemptNumber;
            return this;
        }

        /**
         * Hook invoked on every {@link TaskContext#checkpoint()} call — use to
         * simulate a lease being lost, a pause landing, etc. at a chosen checkpoint.
         */
        public Builder onCheckpoint(Runnable onCheckpoint) {
            this.onCheckpoint = onCheckpoint;
            return this;
        }

        public FakeTaskContext build() {
            return new FakeTaskContext(this);
        }
    }
}
