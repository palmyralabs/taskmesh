package com.palmyralabs.taskmesh.api;

import java.time.Instant;

public record HistoryEvent(
        TaskRef taskRef,
        int attemptCount,
        int retryCount,
        HistoryEventType type,
        String detail,
        Instant occurredAt,
        String nodeId) {
}
