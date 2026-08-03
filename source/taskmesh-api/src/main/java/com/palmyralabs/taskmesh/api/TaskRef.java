package com.palmyralabs.taskmesh.api;

/**
 * {@code partitionKey} is {@code null} for every tier except taskmesh-max,
 * where it's caller-supplied and required — taskmesh never computes or
 * derives it. Kept on this shared record (not a taskmesh-max-only subtype)
 * because the engine never branches on which tier is wired; a tier-specific
 * {@code TaskRef} would force an {@code instanceof} check into
 * tier-agnostic construction sites (D32).
 */
public record TaskRef(String taskType, String taskId, String partitionKey) {

    public TaskRef(String taskType, String taskId) {
        this(taskType, taskId, null);
    }
}
