package com.palmyralabs.taskmesh.api;

public record ReleaseRequest(TerminalDisposition disposition, String reasonCode, String errorSummary) {

    public static ReleaseRequest completed() {
        return new ReleaseRequest(TerminalDisposition.COMPLETED, null, null);
    }

    public static ReleaseRequest requeue(String reasonCode, String errorSummary) {
        return new ReleaseRequest(TerminalDisposition.REQUEUE, reasonCode, errorSummary);
    }

    public static ReleaseRequest failedTerminal(String reasonCode, String errorSummary) {
        return new ReleaseRequest(TerminalDisposition.FAILED_TERMINAL, reasonCode, errorSummary);
    }
}
