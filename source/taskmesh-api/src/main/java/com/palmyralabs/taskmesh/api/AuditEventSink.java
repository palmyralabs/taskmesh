package com.palmyralabs.taskmesh.api;

/**
 * Meaningful operator actions only — never routine automatic state transitions.
 * See docs/planning/interfaces.md.
 */
public interface AuditEventSink {

    void record(AuditEvent event);
}
