package com.palmyralabs.taskmesh.api;

public record ReleaseRequest(TerminalDisposition disposition, String reasonCode, String errorSummary) {

    public static ReleaseRequest completed() {
        return new ReleaseRequest(TerminalDisposition.COMPLETED, null, null);
    }

    /** Retryable failure — burns one unit of retry budget and applies backoff. */
    public static ReleaseRequest retry(String reasonCode, String errorSummary) {
        return new ReleaseRequest(TerminalDisposition.RETRY, reasonCode, errorSummary);
    }

    /**
     * Cooperative hand-back of unfinished work — no budget consumed, no
     * backoff gate. Named {@code yielded} rather than {@code yield} because
     * {@code yield} is a restricted identifier in Java and cannot be invoked
     * unqualified.
     */
    public static ReleaseRequest yielded(String reasonCode, String errorSummary) {
        return new ReleaseRequest(TerminalDisposition.YIELD, reasonCode, errorSummary);
    }

    public static ReleaseRequest failedTerminal(String reasonCode, String errorSummary) {
        return new ReleaseRequest(TerminalDisposition.FAILED_TERMINAL, reasonCode, errorSummary);
    }
}
