package com.palmyralabs.taskmesh.api;

public interface TaskProcessor {

    String taskType();

    void process(TaskContext ctx) throws Exception;
}
