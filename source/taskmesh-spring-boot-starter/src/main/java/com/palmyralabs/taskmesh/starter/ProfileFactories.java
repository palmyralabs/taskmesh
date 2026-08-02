package com.palmyralabs.taskmesh.starter;

import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.HistoryWriter;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.impl.AttemptHistoryWriter;
import com.palmyralabs.taskmesh.impl.ExplicitHeartbeatStrategy;
import com.palmyralabs.taskmesh.impl.HistoryLevel;
import com.palmyralabs.taskmesh.impl.JpaHeartbeatStrategy;
import com.palmyralabs.taskmesh.impl.JpaHistoryWriter;
import com.palmyralabs.taskmesh.impl.MultiNodeTaskLedger;
import com.palmyralabs.taskmesh.impl.SimpleTaskLedger;
import com.palmyralabs.taskmesh.impl.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.impl.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.impl.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.impl.repository.TmTaskRepository;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

/**
 * Shared by {@link TaskmeshEngineAutoConfiguration} and {@link TaskmeshProfileBuilder}
 * — the interfaces-to-implementations mapping per {@link WorkloadProfile}
 * (D28/D29 in decisions.md). Kept as one place so the two entry points
 * (auto-config, manual builder) can never drift apart on what a profile
 * means. Each piece is independent on purpose (not bundled into one object)
 * so a consumer can override exactly one via {@code @ConditionalOnMissingBean}
 * without losing the profile default for the others.
 */
final class ProfileFactories {

    private ProfileFactories() {
    }

    /** Result also implements {@link com.palmyralabs.taskmesh.api.TaskLedger} — callers needing both cast the same instance, never build two. */
    static ClaimStrategy claimStrategy(WorkloadProfile profile, TaskmeshProperties properties,
                                        TmTaskRepository taskRepository, TmInFlightRepository inFlightRepository,
                                        TransactionTemplate transactionTemplate, NodeInfoProvider nodeInfoProvider, RetryPolicy retryPolicy) {
        if (profile == WorkloadProfile.BATCH) {
            SimpleTaskLedger simple = new SimpleTaskLedger(taskRepository, inFlightRepository, transactionTemplate,
                    nodeInfoProvider, retryPolicy);
            simple.recoverOwnClaims();
            return simple;
        }
        return new MultiNodeTaskLedger(taskRepository, inFlightRepository, transactionTemplate, nodeInfoProvider, retryPolicy,
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
