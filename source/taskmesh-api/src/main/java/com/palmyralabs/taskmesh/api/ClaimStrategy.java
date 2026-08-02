package com.palmyralabs.taskmesh.api;

/**
 * Node identity (which node is claiming/renewing/releasing) is an
 * implementation-construction concern, not part of this contract — a given
 * {@code ClaimStrategy} instance is constructed once per node/engine
 * instance and is expected to bind its own node id internally. See D17 in
 * docs/planning/decisions.md.
 */
public interface ClaimStrategy {

    ClaimOutcome claim(TaskRef taskRef);

    RenewOutcome renew(TaskRef taskRef);

    /**
     * Retry-exhaustion (has this task used up its retry budget?) is decided here,
     * atomically with whatever counter that decision reads — never precomputed
     * and passed in via {@link ReleaseRequest}. See docs/planning/claim-strategy.md.
     */
    ReleaseOutcome release(TaskRef taskRef, ReleaseRequest request);
}
