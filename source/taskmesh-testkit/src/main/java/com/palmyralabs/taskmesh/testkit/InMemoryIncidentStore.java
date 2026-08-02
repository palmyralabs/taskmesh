package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.Incident;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.TaskRef;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryIncidentStore implements IncidentStore {

    private final Map<TaskRef, Incident> open = new ConcurrentHashMap<>();
    private final List<Incident> everOpened = new CopyOnWriteArrayList<>();

    @Override
    public void open(Incident incident) {
        open.put(incident.taskRef(), incident);
        everOpened.add(incident);
    }

    @Override
    public void resolve(TaskRef taskRef, String resolvedBy) {
        open.remove(taskRef);
    }

    public Map<TaskRef, Incident> openIncidents() {
        return Map.copyOf(open);
    }

    public List<Incident> everOpened() {
        return List.copyOf(everOpened);
    }
}
