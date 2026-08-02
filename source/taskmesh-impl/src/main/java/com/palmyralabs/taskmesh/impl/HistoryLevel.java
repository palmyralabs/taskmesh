package com.palmyralabs.taskmesh.impl;

import com.palmyralabs.taskmesh.api.HistoryEventType;

import java.util.EnumSet;
import java.util.Set;

/**
 * How much of a task's lifecycle {@link JpaHistoryWriter} actually persists
 * — profiles.md's {@code HistoryWriter} differentiation (BATCH's coarse
 * {@code AttemptHistoryWriter}, HIGH_VOLUME/REACTIVE's {@code LeveledEventHistoryWriter}
 * at {@code STANDARD}/{@code MINIMAL}) unified into one class + level, since
 * the actual difference between them is which {@link HistoryEventType}s get
 * written, not the write mechanism itself.
 */
public enum HistoryLevel {

    /** Terminal outcomes only — lowest write volume, for high-event-rate REACTIVE workloads. */
    MINIMAL(EnumSet.of(HistoryEventType.COMPLETED, HistoryEventType.FAILED, HistoryEventType.RETRIED)),

    /** + STARTED/OWNERSHIP_LOST/PAUSED — one row per attempt boundary, BATCH's coarse mode. */
    STANDARD(EnumSet.of(HistoryEventType.STARTED, HistoryEventType.COMPLETED, HistoryEventType.FAILED,
            HistoryEventType.RETRIED, HistoryEventType.OWNERSHIP_LOST, HistoryEventType.PAUSED)),

    /** Everything, including HEARTBEAT — HIGH_VOLUME's Kafka-durable-audit-log requirement. */
    VERBOSE(EnumSet.allOf(HistoryEventType.class));

    private final Set<HistoryEventType> recorded;

    HistoryLevel(Set<HistoryEventType> recorded) {
        this.recorded = recorded;
    }

    boolean records(HistoryEventType type) {
        return recorded.contains(type);
    }
}
