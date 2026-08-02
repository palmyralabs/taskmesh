package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.AuditEvent;
import com.palmyralabs.taskmesh.api.AuditEventSink;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryAuditEventSink implements AuditEventSink {

    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void record(AuditEvent event) {
        events.add(event);
    }

    public List<AuditEvent> events() {
        return List.copyOf(events);
    }
}
