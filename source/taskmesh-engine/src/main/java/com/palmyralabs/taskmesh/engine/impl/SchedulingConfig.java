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
