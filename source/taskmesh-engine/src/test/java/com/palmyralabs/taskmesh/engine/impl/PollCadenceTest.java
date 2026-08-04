package com.palmyralabs.taskmesh.engine.impl;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The poll cadence control law and its hard bounds, exercised without threads, a scheduler or a
 * database — the reason it is a separate class. Time and the consumption counter are both supplied
 * by the caller, so every case here is deterministic.
 */
class PollCadenceTest {

    private static final int REGULAR_CAPACITY = 251;   // a 256-deep queue minus the priority lane
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private static PollCadence cadence(long minMillis, long maxMillis) {
        PollCadence cadence = new PollCadence(SchedulingConfig.builder()
                .queueCapacity(256)
                .pollMinMillis(minMillis)
                .pollMaxMillis(maxMillis)
                .pollTargetDrainFraction(0.5)
                .build(), REGULAR_CAPACITY, "node-1");
        cadence.reset(0, 0);
        return cadence;
    }

    private static PollCadence cadence() {
        return cadence(1_000, 30_000);
    }

    /** Settle the EWMA at a steady rate so assertions are about the law, not the warm-up. */
    private static long settleAt(PollCadence cadence, long tasksPerSecond) {
        long delay = 0;
        long consumed = 0;
        for (int second = 1; second <= 40; second++) {
            consumed += tasksPerSecond;
            delay = cadence.nextDelayMillis(consumed, second * SECOND);
        }
        return delay;
    }

    // ---- cadence ---------------------------------------------------------------

    /**
     * Backing off is rate-limited: one idle cycle must not strand a node at the ceiling when work
     * is about to arrive. Observed for real — the cadence jumped 1s to 30s the instant the paused
     * enqueue phase started, which would have cost up to 30s of dead time once the pause lifted.
     */
    @Test
    void anIdleQueueEasesOffInsteadOfLeapingToTheCeiling() {
        PollCadence cadence = cadence();

        assertEquals(2_000, cadence.nextDelayMillis(0, SECOND), "at most a doubling per idle cycle");
        assertEquals(4_000, cadence.nextDelayMillis(0, 2 * SECOND));
        assertEquals(8_000, cadence.nextDelayMillis(0, 3 * SECOND));
    }

    @Test
    void aQueueIdleLongEnoughDoesReachTheCeiling() {
        PollCadence cadence = cadence();
        long delay = 0;
        for (int cycle = 1; cycle <= 10; cycle++) {
            delay = cadence.nextDelayMillis(0, cycle * SECOND);
        }
        assertEquals(30_000, delay, "1s,2s,4s,8s,16s,30s — sustained idleness still gets there");
    }

    /**
     * The other half of the asymmetry, and the one that matters most: work arriving after a long
     * idle period must be picked up on the very next cycle, not eased into over several.
     */
    @Test
    void workArrivingAfterIdlenessRecoversImmediatelyRatherThanEasingBack() {
        PollCadence cadence = cadence();
        for (int cycle = 1; cycle <= 10; cycle++) {
            cadence.nextDelayMillis(0, cycle * SECOND);
        }
        assertEquals(30_000, cadence.currentDelayMillis(), "precondition: parked at the ceiling");

        // One cycle's worth of real consumption.
        long recovered = cadence.nextDelayMillis(25, 11 * SECOND);

        assertTrue(recovered < 10_000,
                "recovery must not be rate-limited the way backoff is, got " + recovered + "ms");
    }

    /**
     * The load this engine actually produced: ~182 tasks/s per node. Half of a 251-deep lane at
     * that rate is ~690ms — below the hard floor, so the floor is what applies.
     */
    @Test
    void aFastDrainIsHeldAtTheOneSecondFloor() {
        assertEquals(1_000, settleAt(cadence(), 182),
                "~690ms is what the law wants; the hard floor overrides it");
    }

    /** ~25 tasks/s wants half of 251 rows ≈ 5s — comfortably inside the bounds, so the law wins. */
    @Test
    void aModerateDrainPollsAtTheRateTheLawComputes() {
        long delay = settleAt(cadence(), 25);

        assertTrue(delay >= 4_000 && delay <= 6_000, "expected ~5s for 25 tasks/s, got " + delay);
    }

    /** A 2-second processor drains ~4 tasks/s per node; the configured ceiling should win. */
    @Test
    void aSlowDrainBacksOffToTheCeilingInsteadOfQueryingForNothing() {
        assertEquals(30_000, settleAt(cadence(), 4), "62s of work is queued — back off");
    }

    @Test
    void anExtremeDrainIsHeldAtTheFloorRatherThanSpinning() {
        assertEquals(1_000, settleAt(cadence(), 500_000),
                "the floor is what stops a deep backlog becoming a database spin loop");
    }

    /**
     * Why the rate is smoothed rather than taken from the last sample: one empty cycle must not
     * slam the cadence to the ceiling and strand a queue that is still busy.
     */
    @Test
    void oneQuietCycleDoesNotCollapseAnEstablishedCadence() {
        PollCadence cadence = cadence(1_000, 600_000);
        settleAt(cadence, 25);

        long afterStall = cadence.nextDelayMillis(25L * 40, 41 * SECOND);

        assertTrue(afterStall < 600_000, "a single stall must not jump straight to the ceiling");
    }

    // ---- hard bounds, not overridable ------------------------------------------

    @Test
    void configurationCannotPollFasterThanOneSecond() {
        assertEquals(1_000, cadence(1, 600_000).nextDelayMillis(1_000_000, SECOND),
                "a 1ms configured floor must still be held at the hard 1s bound");
    }

    @Test
    void configurationCannotBackOffBeyondTenMinutes() {
        PollCadence cadence = cadence(1_000, 3_600_000);   // a configured one-hour ceiling
        long delay = 0;
        for (int cycle = 1; cycle <= 40; cycle++) {
            delay = cadence.nextDelayMillis(0, cycle * SECOND);
        }
        assertEquals(600_000, delay, "however long it idles, the hard 600s bound holds");
    }

    /** A configured max below the floor would make the result depend on clamp order. */
    @Test
    void anInvertedMinMaxPairStillYieldsAUsableDelay() {
        assertEquals(5_000, cadence(5_000, 2_000).nextDelayMillis(0, SECOND));
    }

    @Test
    void pollMaxOfZeroFallsBackToTheConfiguredQueryInterval() {
        PollCadence derived = new PollCadence(SchedulingConfig.builder()
                .queueCapacity(256).pollMinMillis(1_000).pollMaxMillis(0).queryIntervalSeconds(3).build(),
                REGULAR_CAPACITY, "node-1");
        derived.reset(0, 0);

        long delay = 0;
        for (int cycle = 1; cycle <= 5; cycle++) {
            delay = derived.nextDelayMillis(0, cycle * SECOND);   // idle: eases 1s -> 2s -> ceiling
        }
        assertEquals(3_000, delay, "0 means 'use queryIntervalSeconds' as the ceiling");
    }

    // ---- pagination ------------------------------------------------------------

    @Test
    void aFullQueueCostsNoQueryAtAll() {
        assertEquals(0, cadence().pageSize(0, 256));
    }

    @Test
    void pageSizeNeverDropsBelowSixteenEvenWhenTheQueueIsNearlyFull() {
        assertEquals(16, cadence().pageSize(3, 256),
                "a round trip returning three rows costs more than the surplus the offer refuses");
    }

    @Test
    void pageSizeNeverExceedsFiveHundredAndTwelve() {
        assertEquals(512, cadence().pageSize(10_000, 100_000),
                "a configured batch of 100k must still be held at the hard 512 bound");
    }

    @Test
    void pageSizeFollowsWhicheverOfCapacityOrBatchIsSmaller() {
        assertEquals(251, cadence().pageSize(251, 1_024), "capacity binds");
        assertEquals(100, cadence().pageSize(251, 100), "batchSize binds");
    }
}
