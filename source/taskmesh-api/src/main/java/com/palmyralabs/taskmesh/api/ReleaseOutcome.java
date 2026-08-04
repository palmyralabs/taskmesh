package com.palmyralabs.taskmesh.api;

public enum ReleaseOutcome {
    OK,
    LOST_OWNERSHIP,
    /**
     * A {@code RETRY} request was escalated to {@code FAILED_TERMINAL}
     * internally because the retry budget was exhausted (D16). Lets the
     * caller distinguish "requeued for retry" from "terminally failed" even
     * though both start from the same {@code RETRY} disposition — needed
     * for correct HistoryWriter/MetricsReporter/IncidentStore bookkeeping.
     * See D19 in docs/planning/decisions.md.
     */
    EXHAUSTED
}
