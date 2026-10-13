package com.palmyralabs.taskmesh.api;

public interface PauseController {

    boolean isPauseRequested();

    void requestPause();

    void resume();

    /**
     * Called from {@code TaskMeshEngine.start()} — implementations that poll a
     * backing store (e.g. a DB-cached flag) should begin polling here, not in
     * their constructor, so a node with {@code taskmesh.auto-start: false}
     * (engine bean built and wired, but never started) generates zero
     * background traffic. No-op by default for implementations with nothing
     * to start (e.g. a plain in-memory flag).
     */
    default void start() {
    }

    /** Called from {@code TaskMeshEngine.stop()} — the {@link #start()} counterpart. No-op by default. */
    default void stop() {
    }
}
