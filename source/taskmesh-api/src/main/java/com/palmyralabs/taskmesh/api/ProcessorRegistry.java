package com.palmyralabs.taskmesh.api;

public interface ProcessorRegistry {

    TaskProcessor lookup(String taskType);

    boolean isRegistered(String taskType);
}
