package com.palmyralabs.taskmesh.api;

/**
 * Retry-backoff jitter strategy — shared by every tier's {@code
 * ExponentialBackoffRetryPolicy} implementation and referenced by {@code
 * TaskmeshProperties} (kept here, not nested in a tier-specific class, since
 * config binding is tier-agnostic).
 */
public enum Jitter {
    /** No jitter — the deterministic {@code base * 2^(attempt-1)} delay, capped. */
    NONE,
    /** AWS "full jitter": uniform random in {@code [0, delay]}. */
    FULL,
    /** AWS "equal jitter": {@code delay/2 + uniform random in [0, delay/2]}. */
    EQUAL
}
