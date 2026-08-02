package com.palmyralabs.taskmesh.api;

import java.time.Instant;

public record Incident(TaskRef taskRef, String reasonCode, String message, Instant createdAt) {
}
