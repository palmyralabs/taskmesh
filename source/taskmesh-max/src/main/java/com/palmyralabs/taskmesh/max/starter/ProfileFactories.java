package com.palmyralabs.taskmesh.max.starter;

import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.HistoryWriter;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.max.AttemptHistoryWriter;
import com.palmyralabs.taskmesh.max.ExplicitHeartbeatStrategy;
import com.palmyralabs.taskmesh.max.HistoryLevel;
import com.palmyralabs.taskmesh.max.JpaHeartbeatStrategy;
import com.palmyralabs.taskmesh.max.JpaHistoryWriter;
import com.palmyralabs.taskmesh.max.PartitionedTaskLedger;
import com.palmyralabs.taskmesh.max.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.max.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;
import com.palmyralabs.taskmesh.starter.TaskmeshProperties;
import com.palmyralabs.taskmesh.starter.WorkloadProfile;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

/**
 * Shared by {@link TaskmeshEngineAutoConfiguration} and {@link TaskmeshProfileBuilder}
 * — the interfaces-to-implementations mapping per {@link WorkloadProfile} for
 * this tier. {@code claimStrategy} is unconditional (D32) — this tier ships
 * only {@link PartitionedTaskLedger}, no single-node variant; {@code profile}
 * still selects {@code heartbeatStrategy}/{@code historyWriter}. Kept as one
 * place so the two entry points can never drift apart on what a profile
 * means. Each piece is independent on purpose so a consumer can override
 * exactly one via {@code @ConditionalOnMissingBean} without losing the
 * profile default for the others.
 */
final class ProfileFactories {

    private ProfileFactories() {
    }

    /** Result also implements {@link com.palmyralabs.taskmesh.api.TaskLedger} — callers needing both cast the same instance, never build two. */
    static ClaimStrategy claimStrategy(TaskmeshProperties properties,
                                        TmTaskRepository taskRepository, TmInFlightRepository inFlightRepository,
                                        TransactionTemplate transactionTemplate, NodeInfoProvider nodeInfoProvider, RetryPolicy retryPolicy) {
        return new PartitionedTaskLedger(taskRepository, inFlightRepository, transactionTemplate, nodeInfoProvider, retryPolicy,
                Duration.ofSeconds(properties.getLeaseSeconds()), Duration.ofSeconds(properties.getReclaimSeconds()),
                Duration.ofSeconds(properties.getAffinitySeconds()), Duration.ofSeconds(properties.getClaimTimeoutSeconds()),
                Duration.ofSeconds(properties.getReservationSeconds()));
    }

    static HeartbeatStrategy heartbeatStrategy(WorkloadProfile profile, TaskmeshProperties properties,
                                                ClaimStrategy claimStrategy, IncidentStore incidentStore) {
        if (profile == WorkloadProfile.BATCH) {
            return new ExplicitHeartbeatStrategy(claimStrategy, incidentStore, Duration.ofMinutes(properties.getMissedHeartbeatMinutes()));
        }
        return new JpaHeartbeatStrategy(claimStrategy);
    }

    static HistoryWriter historyWriter(WorkloadProfile profile, TmHistoryEventRepository historyEventRepository,
                                        TmAttemptHistoryRepository attemptHistoryRepository) {
        return switch (profile) {
            case BATCH -> new AttemptHistoryWriter(attemptHistoryRepository);
            case HIGH_VOLUME -> new JpaHistoryWriter(historyEventRepository, HistoryLevel.VERBOSE);
            case REACTIVE -> new JpaHistoryWriter(historyEventRepository, HistoryLevel.MINIMAL);
        };
    }
}
