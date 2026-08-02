package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.Classification;
import com.palmyralabs.taskmesh.api.FailureClassifier;
import com.palmyralabs.taskmesh.exception.NonRetryableException;

/**
 * Default classification (D6): a {@link NonRetryableException} is
 * non-retryable, carrying its own reason code; everything else is
 * retryable-by-default, reason code derived from the exception's simple
 * class name.
 */
public class DefaultFailureClassifier implements FailureClassifier {

    @Override
    public Classification classify(Throwable failure) {
        if (failure instanceof NonRetryableException nre) {
            return new Classification(false, nre.reasonCode());
        }
        return new Classification(true, failure.getClass().getSimpleName());
    }
}
