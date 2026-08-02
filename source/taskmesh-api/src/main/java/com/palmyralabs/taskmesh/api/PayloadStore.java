package com.palmyralabs.taskmesh.api;

/**
 * Adds a write side to {@link PayloadProvider}. Kept as a forward-looking
 * extension point (D23) — no current taskmesh entry point calls
 * {@link #store}, since {@code enqueue}/{@code execute} take no payload
 * argument and rely entirely on {@link PayloadProvider#load} to resolve it.
 */
public interface PayloadStore extends PayloadProvider {

    void store(TaskRef taskRef, Object payload);
}
