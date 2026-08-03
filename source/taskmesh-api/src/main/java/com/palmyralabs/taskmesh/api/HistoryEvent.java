package com.palmyralabs.taskmesh.api;

import java.time.Instant;

public record HistoryEvent(
        TaskRef taskRef,
        int attemptNumber,
        HistoryEventType type,
        String detail,
        Instant occurredAt,
        String nodeId) {
}
