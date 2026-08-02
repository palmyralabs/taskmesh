package com.palmyralabs.taskmesh.api;

public interface IncidentStore {

    void open(Incident incident);

    void resolve(TaskRef taskRef, String resolvedBy);
}
