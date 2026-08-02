package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.TaskRef;

/**
 * Wraps a {@code PayloadProvider.load()} failure — {@code TaskContext.payload()}
 * can't declare a checked exception, and this surfaces from inside
 * {@code TaskProcessor.process()} like any other processing failure, routed
 * through the normal {@code FailureClassifier}/retry path (D21).
 */
public class PayloadLoadException extends RuntimeException {

    public PayloadLoadException(TaskRef taskRef, Throwable cause) {
        super("Failed to load payload for " + taskRef, cause);
    }
}
