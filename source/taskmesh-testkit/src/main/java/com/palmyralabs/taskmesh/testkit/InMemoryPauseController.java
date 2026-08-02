package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.PauseController;

/**
 * Cluster-flag style (matches {@code ClusterFlagPauseController} in
 * profiles.md) — a single in-memory boolean, no per-task_type scoping.
 */
public class InMemoryPauseController implements PauseController {

    private volatile boolean pauseRequested = false;

    @Override
    public boolean isPauseRequested() {
        return pauseRequested;
    }

    @Override
    public void requestPause() {
        pauseRequested = true;
    }

    @Override
    public void resume() {
        pauseRequested = false;
    }
}
