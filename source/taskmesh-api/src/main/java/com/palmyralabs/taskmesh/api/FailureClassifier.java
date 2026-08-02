package com.palmyralabs.taskmesh.api;

public interface FailureClassifier {

    Classification classify(Throwable failure);
}
