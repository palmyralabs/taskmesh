package com.palmyralabs.taskmesh.starter;

/**
 * Which taskmesh implementation set {@link TaskmeshEngineAutoConfiguration}
 * (or {@link TaskmeshProfileBuilder}, for manual wiring) selects — see
 * {@code docs/planning/profiles.md} and decisions D28/D29 in the taskmesh
 * repo. Selecting a profile picks concrete implementation classes
 * (ClaimStrategy/TaskLedger, HeartbeatStrategy, HistoryWriter); the numeric
 * tuning knobs ({@link TaskmeshProperties}) stay independently configurable
 * regardless of profile.
 */
public enum WorkloadProfile {
    BATCH,
    HIGH_VOLUME,
    REACTIVE
}
