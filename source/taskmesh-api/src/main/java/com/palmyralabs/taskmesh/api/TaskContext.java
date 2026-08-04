package com.palmyralabs.taskmesh.api;

/**
 * Handed to {@link TaskProcessor#process} for the duration of one execution.
 * Identity, counters and payload come from {@link TaskInfo}; what this adds is
 * the live-execution lifecycle — none of which is meaningful once the task has
 * gone terminal, which is why {@link TerminalContext} is a separate type.
 */
public interface TaskContext extends TaskInfo {

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
