package com.palmyralabs.taskmesh.api;

/**
 * Audit-only. Never read by control flow — retry/claim decisions must never depend
 * on anything written here. See docs/planning/interfaces.md.
 */
public interface HistoryWriter {

    void record(HistoryEvent event);
}
