package com.palmyralabs.taskmesh.engine.impl;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Worker-pool / dispatch tuning knobs. Getters/setters (for YAML/Spring
 * binding by a future taskmesh-spring-boot-starter) plus a {@link #builder()}
 * (for plain programmatic construction) — per configuration.md's dual-path
 * rule. Defaults here are engine-level fallbacks, not profile-tuned; profile
 * modules (taskmesh-profile-*) override them, per profiles.md.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SchedulingConfig {

    @Builder.Default
    private int workerPoolSize = 8;

    @Builder.Default
    private int queueCapacity = 100;

    @Builder.Default
    private int batchSize = 50;

    @Builder.Default
    private int queryIntervalSeconds = 5;

    /**
     * Derive the poll cadence from the rate work is actually leaving the execution queue,
     * instead of polling on a fixed {@link #queryIntervalSeconds} timer.
     *
     * <p>A fixed interval has to be sized against {@code workerPoolSize / perExecutionTime},
     * which nobody knows in advance and which changes with the workload: too long and workers
     * idle in front of a full backlog (at the old 10s default with ~44ms executions, a node ran
     * at roughly 5% of its worker capacity), too short and it hammers the database for nothing.
     * Adaptive polling measures the drain rate and comes back when the queue is about half
     * empty, so the cadence follows the workload rather than a guess.
     *
     * <p>The query size needs no separate knob: the poller already asks for
     * {@code min(remaining queue capacity, batchSize)}, so it naturally shrinks as the queue
     * fills and {@code batchSize} becomes a ceiling rather than a fixed size.
     */
    @Builder.Default
    private boolean adaptivePolling = true;

    /** Floor on the adaptive cadence — stops a deep backlog turning into a spin loop. */
    @Builder.Default
    private long pollMinMillis = 50;

    /** Ceiling on the adaptive cadence. 0 means "use {@link #queryIntervalSeconds}". */
    @Builder.Default
    private long pollMaxMillis = 0;

    /**
     * Re-poll when this fraction of the regular lane has drained. Half leaves headroom for the
     * poll itself to complete before workers run dry, without refilling so often that each
     * cycle returns a handful of rows.
     */
    @Builder.Default
    private double pollTargetDrainFraction = 0.5;

    @Builder.Default
    private int safetyNetPollSeconds = 30;

    /** BacklogPromoter.promote() batch size per task type, per cycle. */
    @Builder.Default
    private int backlogBatchSize = 50;

    /** BacklogPromoter.promote() poll interval. */
    @Builder.Default
    private int backlogIntervalSeconds = 5;

    /** Fixed capacity of the execution queue's priorityArray (D25). A constant for now, per that decision. */
    public static final int PRIORITY_QUEUE_CAPACITY = 5;

    /** How long execute()'s priority push waits for a priorityArray slot before rejecting (D25). */
    @Builder.Default
    private long priorityOfferTimeoutMillis = 2000;
}
