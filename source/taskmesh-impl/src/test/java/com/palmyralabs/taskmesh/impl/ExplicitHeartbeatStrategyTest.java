package com.palmyralabs.taskmesh.impl;

import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.Incident;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.RenewOutcome;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** BATCH profile's active-hang-detection HeartbeatStrategy (D28/infra-implementations.md Variant A). */
class ExplicitHeartbeatStrategyTest {

    private static class RecordingIncidentStore implements IncidentStore {
        final List<Incident> opened = new ArrayList<>();

        @Override
        public void open(Incident incident) {
            opened.add(incident);
        }

        @Override
        public void resolve(TaskRef taskRef, String resolvedBy) {
        }
    }

    private static class FixedClaimStrategy implements ClaimStrategy {
        RenewOutcome nextOutcome = RenewOutcome.RENEWED;

        @Override
        public com.palmyralabs.taskmesh.api.ClaimOutcome claim(TaskRef taskRef) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RenewOutcome renew(TaskRef taskRef) {
            return nextOutcome;
        }

        @Override
        public com.palmyralabs.taskmesh.api.ReleaseOutcome release(TaskRef taskRef, com.palmyralabs.taskmesh.api.ReleaseRequest request) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void firstCheckpointNeverFlagsAnIncident() {
        FixedClaimStrategy claimStrategy = new FixedClaimStrategy();
        RecordingIncidentStore incidentStore = new RecordingIncidentStore();
        ExplicitHeartbeatStrategy strategy = new ExplicitHeartbeatStrategy(claimStrategy, incidentStore, Duration.ofMillis(50));

        assertEquals(RenewOutcome.RENEWED, strategy.onCheckpoint(new TaskRef("T", "1")));
        assertTrue(incidentStore.opened.isEmpty(), "no prior checkpoint to compare against — nothing to flag");
    }

    @Test
    void gapUnderThresholdDoesNotFlag() throws InterruptedException {
        FixedClaimStrategy claimStrategy = new FixedClaimStrategy();
        RecordingIncidentStore incidentStore = new RecordingIncidentStore();
        ExplicitHeartbeatStrategy strategy = new ExplicitHeartbeatStrategy(claimStrategy, incidentStore, Duration.ofSeconds(10));
        TaskRef ref = new TaskRef("T", "2");

        strategy.onCheckpoint(ref);
        Thread.sleep(20);
        strategy.onCheckpoint(ref);

        assertTrue(incidentStore.opened.isEmpty());
    }

    @Test
    void gapOverThresholdOpensAnIncidentButStillRenews() throws InterruptedException {
        FixedClaimStrategy claimStrategy = new FixedClaimStrategy();
        RecordingIncidentStore incidentStore = new RecordingIncidentStore();
        ExplicitHeartbeatStrategy strategy = new ExplicitHeartbeatStrategy(claimStrategy, incidentStore, Duration.ofMillis(20));
        TaskRef ref = new TaskRef("T", "3");

        strategy.onCheckpoint(ref);
        Thread.sleep(60);
        RenewOutcome outcome = strategy.onCheckpoint(ref);

        assertEquals(RenewOutcome.RENEWED, outcome, "flagging a missed heartbeat must not block the renewal itself");
        assertEquals(1, incidentStore.opened.size());
        assertEquals("MISSED_HEARTBEAT", incidentStore.opened.get(0).reasonCode());
    }

    @Test
    void lostRenewalClearsTrackingForThatRef() throws InterruptedException {
        FixedClaimStrategy claimStrategy = new FixedClaimStrategy();
        RecordingIncidentStore incidentStore = new RecordingIncidentStore();
        ExplicitHeartbeatStrategy strategy = new ExplicitHeartbeatStrategy(claimStrategy, incidentStore, Duration.ofMillis(20));
        TaskRef ref = new TaskRef("T", "4");

        strategy.onCheckpoint(ref);
        claimStrategy.nextOutcome = RenewOutcome.LOST;
        strategy.onCheckpoint(ref);

        // A later, unrelated checkpoint for the same ref (e.g. a stale caller still holding a reference) must not
        // be compared against the pre-loss timestamp and spuriously flag a huge gap.
        claimStrategy.nextOutcome = RenewOutcome.RENEWED;
        Thread.sleep(60);
        strategy.onCheckpoint(ref);

        assertTrue(incidentStore.opened.isEmpty(), "tracking must reset after ownership is lost, not compare across an ownership boundary");
    }
}
