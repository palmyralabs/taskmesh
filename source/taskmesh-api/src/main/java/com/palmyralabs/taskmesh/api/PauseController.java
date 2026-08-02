package com.palmyralabs.taskmesh.api;

public interface PauseController {

    boolean isPauseRequested();

    void requestPause();

    void resume();
}
