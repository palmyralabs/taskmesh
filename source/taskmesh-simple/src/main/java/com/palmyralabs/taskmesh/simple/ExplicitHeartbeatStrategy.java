package com.palmyralabs.taskmesh.simple;

import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.Incident;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.TaskRef;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BATCH profile's {@code HeartbeatStrategy} (infra-implementations.md
 * Variant A, cleanser-style active hang detection) — still renews the claim
 * via {@link ClaimStrategy#renew} at every checkpoint like {@link JpaHeartbeatStrategy}
 * does, but additionally tracks the gap between successive checkpoints for
 * a given {@link TaskRef} and opens an incident the moment that gap exceeds
 * {@code missedHeartbeatThreshold} — flagging a task that's gone quiet for
 * longer than expected, on top of (not instead of) ordinary lease-expiry
 * eventually reclaiming it. Detection is reactive (checked at the next
 * checkpoint call), not a background timer — deliberately, so this needs no
 * new engine lifecycle hooks (onClaimed/onReleased don't exist today).
 *
 * <p>{@code lastCheckpoint} is cleared once a {@link RenewOutcome#LOST}
 * comes back (ownership gone, nothing left to watch) — otherwise entries
 * for genuinely long-running BATCH jobs are expected to stay few enough
 * (a handful of heavy jobs, per profiles.md) that no further eviction is needed.
 */
public class ExplicitHeartbeatStrategy implements HeartbeatStrategy {

    private final ClaimStrategy claimStrategy;
    private final IncidentStore incidentStore;
    private final Duration missedHeartbeatThreshold;

    private final ConcurrentMap<TaskRef, Instant> lastCheckpoint = new ConcurrentHashMap<>();

    public ExplicitHeartbeatStrategy(ClaimStrategy claimStrategy, IncidentStore incidentStore, Duration missedHeartbeatThreshold) {
        this.claimStrategy = claimStrategy;
        this.incidentStore = incidentStore;
        this.missedHeartbeatThreshold = missedHeartbeatThreshold;
    }

    @Override
    public RenewOutcome onCheckpoint(TaskRef taskRef) {
        Instant now = Instant.now();
        Instant previous = lastCheckpoint.put(taskRef, now);
        if (previous != null) {
            Duration gap = Duration.between(previous, now);
            if (gap.compareTo(missedHeartbeatThreshold) > 0) {
                incidentStore.open(new Incident(taskRef, "MISSED_HEARTBEAT",
                        "No checkpoint for " + gap + " (threshold " + missedHeartbeatThreshold + ")", now));
            }
        }

        RenewOutcome outcome = claimStrategy.renew(taskRef);
        if (outcome == RenewOutcome.LOST) {
            lastCheckpoint.remove(taskRef);
        }
        return outcome;
    }
}
