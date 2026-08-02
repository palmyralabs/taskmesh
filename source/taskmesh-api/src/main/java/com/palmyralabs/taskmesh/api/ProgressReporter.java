package com.palmyralabs.taskmesh.api;

public interface ProgressReporter {

    void report(long itemsProcessed, long itemsFailed);

    void heartbeat();
}
