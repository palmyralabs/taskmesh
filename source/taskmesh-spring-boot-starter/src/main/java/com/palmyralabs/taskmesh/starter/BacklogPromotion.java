package com.palmyralabs.taskmesh.starter;

/**
 * When a task's hot {@code tm_in_flight} claim row is created, relative to the
 * cold {@code tm_task} row — the {@code BacklogPromoter} selector for
 * taskmesh-max ({@code taskmesh.backlog-promotion}).
 *
 * <p>Tier-agnostic like the rest of this module: taskmesh-simple/-pro create the
 * cold/hot pair eagerly inside {@code enqueue()} itself and always wire
 * {@code NoopBacklogPromoter}, so this setting does nothing there. Only
 * taskmesh-max defers the hot row to a promoter and can therefore honour it.
 */
public enum BacklogPromotion {

    /**
     * Hot row created immediately at enqueue ({@code NoopBacklogPromoter}) — the
     * default, and the behaviour every tier had before promotion was configurable.
     * {@code tm_in_flight} tracks the whole non-terminal backlog.
     */
    EAGER,

    /**
     * Hot rows created in bounded batches by {@code JpaBacklogPromoter}, which
     * holds {@code tm_in_flight} inside the
     * {@code inFlightLowWatermark}..{@code inFlightHighWatermark} window rather
     * than letting it grow to match the backlog. Costs a promotion round trip and
     * means a freshly enqueued task is not claimable until it is promoted —
     * including on the {@code execute()} priority path, which operates on
     * {@code tm_in_flight}.
     */
    THROTTLED
}
