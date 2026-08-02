package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.HistoryEvent;
import com.palmyralabs.taskmesh.api.HistoryWriter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryHistoryWriter implements HistoryWriter {

    private final List<HistoryEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void record(HistoryEvent event) {
        events.add(event);
    }

    public List<HistoryEvent> events() {
        return List.copyOf(events);
    }
}
