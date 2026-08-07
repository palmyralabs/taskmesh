package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.AttemptCounters;
import com.palmyralabs.taskmesh.api.ErrorLogger;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.PauseController;
import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.ProgressReporter;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.api.TransactionRunner;
import com.palmyralabs.taskmesh.exception.NonRetryableException;

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
    private final AttemptCounters counters;
    private final PayloadProvider payloadProvider;
    private final HeartbeatStrategy heartbeatStrategy;
    private final TransactionRunner transactionRunner;
    private final PauseController pauseController;
    private final BooleanSupplier shutdownRequested;
    private final ProgressReporter progressReporter;
    private final ErrorLogger errorLogger;

    private boolean payloadLoaded = false;
    private Object payload;

    private volatile boolean ownershipValid = true;

    DefaultTaskContext(TaskRef taskRef, AttemptCounters counters, PayloadProvider payloadProvider, HeartbeatStrategy heartbeatStrategy,
                        PauseController pauseController, BooleanSupplier shutdownRequested,
                        TransactionRunner transactionRunner) {
        this.taskRef = taskRef;
        this.counters = counters;
        this.payloadProvider = payloadProvider;
        this.heartbeatStrategy = heartbeatStrategy;
        this.transactionRunner = transactionRunner;
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
    public int attemptCount() {
        return counters.attemptCount();
    }

    @Override
    public int retryCount() {
        return counters.retryCount();
    }

    @Override
    public Object payload() {
        if (!payloadLoaded) {
            try {
                payload = payloadProvider.load(taskRef);
            } catch (NonRetryableException e) {
                // Preserve identity: FailureClassifier pattern-matches on the
                // exception type it receives, not its cause chain. Wrapping
                // this in PayloadLoadException would silently downgrade a
                // "don't retry" signal from the payload provider into the
                // engine's default retryable classification.
                throw e;
            } catch (Exception e) {
                throw new PayloadLoadException(taskRef, e);
            }
            payloadLoaded = true;
        }
        return payload;
    }

    /**
     * The payload if this execution already loaded it, {@code null} otherwise —
     * never triggering a load. Used to build the {@code TerminalContext}: the
     * terminal transition runs inside the ledger's transaction, and fetching a
     * payload there purely to report an outcome would put consumer I/O on that
     * path for no benefit.
     */
    Object payloadIfLoaded() {
        return payloadLoaded ? payload : null;
    }

    @Override
    public void checkpoint() {
        if (!ownershipValid) {
            return;
        }
        // Its own short transaction: the renewal reaches ClaimStrategy#renew, which opens none of
        // its own, and a checkpoint fires in the middle of the processor's work where no engine
        // transaction is (or should be) open.
        RenewOutcome outcome = transactionRunner.call(() -> heartbeatStrategy.onCheckpoint(taskRef));
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
