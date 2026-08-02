package com.palmyralabs.taskmesh.exception;

public class NonRetryableException extends RuntimeException {

    private final String reasonCode;

    public NonRetryableException(String reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }

    public NonRetryableException(String reasonCode, String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = reasonCode;
    }

    public String reasonCode() {
        return reasonCode;
    }
}
