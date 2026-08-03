package com.palmyralabs.taskmesh.starter;

import com.palmyralabs.taskmesh.api.Jitter;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Generic taskmesh configuration — {@code taskmesh.*} — consumed by
 * {@link TaskmeshEngineAutoConfiguration} and available for manual use with
 * {@link TaskmeshProfileBuilder}. Not task-type/consumer specific (moved
 * out of the migration toolkit's own {@code TaskmeshMigrationProperties},
 * D29-successor decision — see decisions.md).
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "taskmesh")
public class TaskmeshProperties {

    /** Selects implementation classes (ClaimStrategy/HeartbeatStrategy/HistoryWriter) — see {@link WorkloadProfile}. */
    private WorkloadProfile profile = WorkloadProfile.HIGH_VOLUME;

    private long leaseSeconds = 300;
    private long reclaimSeconds = 60;
    private long affinitySeconds = 150;
    private long claimTimeoutSeconds = 300;
    /** How long a freshly enqueued QUEUED row stays reserved to the enqueuing node before another node may steal it. */
    private long reservationSeconds = 60;
    private int maxAttempts = 10;
    private long backoffBaseMinutes = 2;
    private long backoffCapMinutes = 60;
    private Jitter backoffJitter = Jitter.NONE;

    private int workerPoolSize = 8;
    private int batchSize = 500;
    private int queryIntervalSeconds = 10;

    /** BATCH profile only ({@code ExplicitHeartbeatStrategy}) — gap since the last checkpoint before an incident is opened. */
    private long missedHeartbeatMinutes = 10;

    /** Blank (default) falls back to taskmesh's DefaultNodeInfoProvider (hostname+PID). */
    private String nodeId = "";
}
