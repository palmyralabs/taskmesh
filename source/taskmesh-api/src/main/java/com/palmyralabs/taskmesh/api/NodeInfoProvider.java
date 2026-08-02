package com.palmyralabs.taskmesh.api;

/**
 * Supplies this process's stable node identity. Constructor-injected wherever
 * {@code nodeId} was previously threaded through as a raw {@code String} —
 * an application wires one {@code NodeInfoProvider} instance into the engine
 * and any {@code ClaimStrategy}/{@code TaskLedger} implementation that needs
 * it, so every collaborator agrees on identity without the application
 * having to invent/copy a node id string itself. {@link #nodeId()} must
 * return the same value for the lifetime of the process.
 */
@FunctionalInterface
public interface NodeInfoProvider {

    String nodeId();
}
