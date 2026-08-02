package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.ErrorLogger;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultErrorLogger implements ErrorLogger {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultErrorLogger.class);

    private final TaskRef taskRef;

    DefaultErrorLogger(TaskRef taskRef) {
        this.taskRef = taskRef;
    }

    @Override
    public void log(String itemId, String message) {
        LOG.warn("{} item={}: {}", taskRef, itemId, message);
    }
}
