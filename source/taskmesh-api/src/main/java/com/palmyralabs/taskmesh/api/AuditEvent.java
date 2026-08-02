package com.palmyralabs.taskmesh.api;

import java.time.Instant;

public record AuditEvent(
        String eventType,
        TaskRef taskRef,
        ActorType actorType,
        String performedBy,
        String message,
        Instant occurredAt) {
}
