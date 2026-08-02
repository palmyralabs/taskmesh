package com.palmyralabs.taskmesh.api;

public interface TaskContext {

    TaskRef taskRef();

    Object payload();

    /**
     * Attempts lease renewal (if applicable) and refreshes ownership/pause/shutdown
     * state. Call at natural progress boundaries; frequency is the processor's own
     * choice and directly determines how quickly a lease-losing task is noticed.
     */
    void checkpoint();

    boolean isOwnershipValid();

    boolean isPauseRequested();

    boolean isShutdownRequested();

    ProgressReporter progress();

    ErrorLogger errorLogger();
}
