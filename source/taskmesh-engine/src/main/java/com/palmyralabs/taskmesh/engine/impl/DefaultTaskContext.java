package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.ErrorLogger;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.PauseController;
import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.ProgressReporter;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskRef;

import java.util.function.BooleanSupplier;

/**
 * The engine's real {@link TaskContext}. {@link #checkpoint()} is the single
 * point that renews the lease (via {@link HeartbeatStrategy}) and refreshes
 * ownership state, matching the interface's own Javadoc contract.
 *
 * <p>{@link #payload()} loads via the candidate's {@link PayloadProvider} on
 * first call and caches the result — a processor that never reads the
 * payload never pays for the load, and repeated reads don't re-fetch/re-decode.
 * A single instance is only ever driven by one worker thread, so no
 * synchronization is needed around the cache.
 */
class DefaultTaskContext implements TaskContext {

    private final TaskRef taskRef;
    private final int attemptNumber;
    private final PayloadProvider payloadProvider;
    private final HeartbeatStrategy heartbeatStrategy;
    private final PauseController pauseController;
    private final BooleanSupplier shutdownRequested;
    private final ProgressReporter progressReporter;
    private final ErrorLogger errorLogger;

    private boolean payloadLoaded = false;
    private Object payload;

    private volatile boolean ownershipValid = true;

    DefaultTaskContext(TaskRef taskRef, int attemptNumber, PayloadProvider payloadProvider, HeartbeatStrategy heartbeatStrategy,
                        PauseController pauseController, BooleanSupplier shutdownRequested) {
        this.taskRef = taskRef;
        this.attemptNumber = attemptNumber;
        this.payloadProvider = payloadProvider;
        this.heartbeatStrategy = heartbeatStrategy;
        this.pauseController = pauseController;
        this.shutdownRequested = shutdownRequested;
        this.progressReporter = new DefaultProgressReporter(this);
        this.errorLogger = new DefaultErrorLogger(taskRef);
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
        if (!payloadLoaded) {
            try {
                payload = payloadProvider.load(taskRef);
            } catch (Exception e) {
                throw new PayloadLoadException(taskRef, e);
            }
            payloadLoaded = true;
        }
        return payload;
    }

    @Override
    public void checkpoint() {
        if (!ownershipValid) {
            return;
        }
        RenewOutcome outcome = heartbeatStrategy.onCheckpoint(taskRef);
        if (outcome == RenewOutcome.LOST) {
            ownershipValid = false;
        }
    }

    @Override
    public boolean isOwnershipValid() {
        return ownershipValid;
    }

    @Override
    public boolean isPauseRequested() {
        return pauseController.isPauseRequested();
    }

    @Override
    public boolean isShutdownRequested() {
        return shutdownRequested.getAsBoolean();
    }

    @Override
    public ProgressReporter progress() {
        return progressReporter;
    }

    @Override
    public ErrorLogger errorLogger() {
        return errorLogger;
    }
}
