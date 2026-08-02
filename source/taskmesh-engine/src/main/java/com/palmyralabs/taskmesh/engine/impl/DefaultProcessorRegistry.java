package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.ProcessorRegistry;
import com.palmyralabs.taskmesh.api.TaskProcessor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Always in-memory — task_type -> TaskProcessor is a construction-time
 * wiring concern, never persistence-backed, so there's no infra variant of
 * this interface (unlike ClaimStrategy/HistoryWriter/etc.). Refuses to
 * construct on a duplicate task_type (D7 — fail loud, not silent).
 */
public class DefaultProcessorRegistry implements ProcessorRegistry {

    private final Map<String, TaskProcessor> processors = new ConcurrentHashMap<>();

    public DefaultProcessorRegistry(List<TaskProcessor> processors) {
        for (TaskProcessor processor : processors) {
            TaskProcessor existing = this.processors.putIfAbsent(processor.taskType(), processor);
            if (existing != null) {
                throw new IllegalArgumentException(
                        "Duplicate TaskProcessor registration for task type '" + processor.taskType() + "': "
                                + existing.getClass().getName() + " and " + processor.getClass().getName());
            }
        }
    }

    @Override
    public TaskProcessor lookup(String taskType) {
        return processors.get(taskType);
    }

    @Override
    public boolean isRegistered(String taskType) {
        return processors.containsKey(taskType);
    }
}
