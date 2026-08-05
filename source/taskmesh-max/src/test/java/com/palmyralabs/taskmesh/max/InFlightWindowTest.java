package com.palmyralabs.taskmesh.max;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The in-flight window control law, exercised with no database and no engine — the reason it is a
 * separate class. Levels and timestamps are supplied by the caller, so every case is deterministic.
 */
class InFlightWindowTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final int INTERVAL = 5;
    private static final int FLOOR = 100;
    private static final int CEILING = 100_000;
    /** Small enough that the queue-derived floor never masks the configured one in these tests. */
    private static final int QUEUE_CAPACITY = 20;

    private static InFlightWindow window() {
        return new InFlightWindow(FLOOR, CEILING, INTERVAL, QUEUE_CAPACITY, "node-1");
    }

    /**
     * Steady state: consumes {@code perCycle} rows per interval and refills every {@code refillEvery}
     * cycles, which is what a node sharing slices with {@code refillEvery} peers experiences.
     */
    private static void run(InFlightWindow window, int perCycle, int cycles, long startLevel, int refillEvery) {
        long level = startLevel;
        for (int cycle = 0; cycle <= cycles; cycle++) {
            window.observe(level, (long) cycle * INTERVAL * SECOND);
            level = Math.max(0, level - perCycle);
            if (refillEvery > 0 && cycle % refillEvery == 0) {
                int refill = perCycle * refillEvery;
                window.promoted(refill);
                level += refill;
            }
        }
    }

    // ---- the window itself -----------------------------------------------------

    @Test
    void beforeTwoSamplesExistItFallsBackToTheConfiguredWatermarks() {
        InFlightWindow window = window();

        window.observe(5_000, 0);
        assertEquals(FLOOR, window.low(), "no history yet — use what was configured");
        assertEquals(CEILING, window.high());

        window.observe(4_500, INTERVAL * SECOND);
        assertEquals(FLOOR, window.low(), "one sample is still not two");
    }

    @Test
    void aSteadyDrainProducesAWindowAroundOneAndAHalfIntervalsOfWork() {
        InFlightWindow window = window();
        run(window, 500, 8, 20_000, 1);

        assertTrue(window.low() >= 700 && window.low() <= 1_600,
                "x=500 at a widening near 1.2-2.0 puts low in the high hundreds, got " + window.low());
        double ratio = (double) window.high() / window.low();
        assertTrue(ratio > 1.2 && ratio < 1.3, "high/low must stay at 1.5/1.2 = 1.25, got " + ratio);
    }

    @Test
    void wideningStartsAtTheSpecifiedSeed() {
        assertEquals(1.6, InFlightWindow.INITIAL_WIDENING, 0.001);
    }

    /**
     * The bug that motivated deriving {@code high} from the clamped {@code low}: clamping both
     * against the configured floor independently collapsed them onto it, leaving no hysteresis
     * band at all. Seen for real as "window 1500..1500" in a 50k run.
     */
    @Test
    void theHysteresisBandSurvivesTheConfiguredFloor() {
        InFlightWindow tight = new InFlightWindow(2_000, 100_000, INTERVAL, QUEUE_CAPACITY, "node-1");
        run(tight, 10, 8, 3_000, 1);   // x=50, so the law wants ~90 — far below the 2000 floor

        assertEquals(2_000, tight.low(), "the configured low is a floor");
        assertTrue(tight.high() > tight.low(),
                "and high must stay above it, not collapse onto it: " + tight.low() + ".." + tight.high());
    }

    @Test
    void theConfiguredHighIsACeiling() {
        InFlightWindow capped = new InFlightWindow(100, 900, INTERVAL, QUEUE_CAPACITY, "node-1");
        run(capped, 5_000, 8, 200_000, 1);   // x=25000, far above the ceiling

        assertTrue(capped.high() <= 900, "configured high is a ceiling, got " + capped.high());
    }

    /**
     * Promotion is not consumption. Without accounting for what the promoter added, the next cycle
     * reads its own refill as demand and the estimate runs away.
     */
    @Test
    void rowsThePromoterAddedAreNotCountedAsConsumption() {
        // A level that never moves while 500 rows are refilled each cycle means 500 rows are being
        // consumed each cycle. Declared, that is exactly what the window should measure; undeclared,
        // a flat level looks like no demand at all.
        InFlightWindow counted = window();
        InFlightWindow blind = window();
        for (int cycle = 0; cycle <= 4; cycle++) {
            counted.observe(1_000, (long) cycle * INTERVAL * SECOND);
            counted.promoted(500);
            blind.observe(1_000, (long) cycle * INTERVAL * SECOND);   // same levels, refill never declared
        }

        assertTrue(counted.low() > blind.low(),
                "declaring the refill should raise measured consumption, " + blind.low() + " vs " + counted.low());
    }

    // ---- X: measured, and smooth ------------------------------------------------

    /**
     * A window whose floor sits above anything the level reaches, so the node wants a refill on
     * every single cycle — i.e. every cycle without one is a genuinely contended cycle.
     */
    private static InFlightWindow alwaysHungry() {
        return new InFlightWindow(5_000, 100_000, INTERVAL, QUEUE_CAPACITY, "node-1");
    }

    /**
     * X exists to cover the cycles this node spends <b>waiting for a slice it wanted</b> — roughly
     * the node count, under contention. Waiting four cycles per refill should converge towards 4.
     */
    @Test
    void wideningConvergesOnTheObservedRefillGap() {
        InFlightWindow window = alwaysHungry();
        run(window, 200, 200, 2_000, 4);

        assertTrue(window.widening() > 2.0 && window.widening() < 6.0,
                "waiting 4 cycles per refill should pull X towards 4, got " + window.widening());
    }

    @Test
    void aNodeRefillingEveryCycleSettlesNearTheFloor() {
        InFlightWindow window = window();
        run(window, 200, 120, 40_000, 1);

        assertTrue(window.widening() < 2.0,
                "no contention means no widening needed, got " + window.widening());
    }

    /**
     * The gap must measure <b>contention</b>, not comfort. A node that stays well above its low
     * mark simply has no reason to refill, and counting those idle cycles as lost races inflates X,
     * which raises the low mark, which means it asks even less often — the same positive feedback
     * the level controller had. Measured for real as gaps of 4-6 cycles across only three nodes.
     */
    @Test
    void cyclesWhereNoRefillWasNeededAreNotCountedAsContention() {
        InFlightWindow window = window();
        // Consumption is tiny relative to the configured floor, so the level sits far above `low`
        // and the node genuinely never needs a refill for long stretches.
        long level = 90_000;
        for (int cycle = 0; cycle <= 120; cycle++) {
            window.observe(level, (long) cycle * INTERVAL * SECOND);
            level = Math.max(0, level - 10);
            if (cycle % 20 == 0) {
                window.promoted(200);
                level += 200;
            }
        }

        assertTrue(window.widening() < 2.5,
                "20 comfortable cycles between refills is not a 20-cycle contention gap, got "
                        + window.widening());
    }

    /**
     * The regression this rewrite is for. The old controller widened on {@code level < low/2} and
     * narrowed on {@code level >= low} — but {@code low} is itself derived from the widening, so it
     * moved the threshold it was judged against and oscillated 1.6 -> 5.4 -> 1.57 -> 5.36 in a real
     * run. Under steady conditions X must now settle, not hunt.
     */
    @Test
    void wideningDoesNotOscillateUnderSteadyLoad() {
        InFlightWindow window = window();
        run(window, 200, 40, 40_000, 2);          // let it settle
        double settled = window.widening();

        double min = settled;
        double max = settled;
        for (int cycle = 41; cycle <= 140; cycle++) {
            window.observe(Math.max(0, 4_000 - (cycle % 2) * 200), (long) cycle * INTERVAL * SECOND);
            if (cycle % 2 == 0) {
                window.promoted(400);
            }
            min = Math.min(min, window.widening());
            max = Math.max(max, window.widening());
        }

        assertTrue(max / min < 1.5,
                "X should settle under steady load, not hunt: swung " + min + " .. " + max);
    }

    /** Movement is blended, so no single cycle can jerk X far in either direction. */
    @Test
    void wideningMovesSmoothlyRatherThanInSteps() {
        InFlightWindow window = window();
        run(window, 200, 40, 40_000, 1);
        double before = window.widening();

        window.observe(0, 200L * INTERVAL * SECOND);   // the sharpest signal there is

        double jump = window.widening() / before;
        assertTrue(jump < 1.2, "even an empty window must not jerk X, moved x" + jump);
        assertTrue(window.widening() > before, "but it should still move up");
    }

    /**
     * The window has to survive the worst plausible gap, not the average. Alternating 1 and 5 has a
     * mean of 3, and a window sized for 3 starves every time a 5 lands — so erratic refill timing
     * must buy headroom that steady timing does not.
     */
    @Test
    void anErraticRefillGapBuysMoreHeadroomThanASteadyOneWithTheSameMean() {
        InFlightWindow steady = alwaysHungry();
        long level = 2_000;
        for (int cycle = 0; cycle <= 200; cycle++) {
            steady.observe(level, (long) cycle * INTERVAL * SECOND);
            level = Math.max(0, level - 200);
            if (cycle % 3 == 0) {           // gap of exactly 3, every time
                steady.promoted(600);
                level += 600;
            }
        }

        InFlightWindow erratic = alwaysHungry();
        level = 2_000;
        boolean shortGap = true;
        int next = 0;
        for (int cycle = 0; cycle <= 200; cycle++) {
            erratic.observe(level, (long) cycle * INTERVAL * SECOND);
            level = Math.max(0, level - 200);
            if (cycle >= next) {            // gaps alternate 1 and 5 — same mean of 3
                erratic.promoted(600);
                level += 600;
                next = cycle + (shortGap ? 1 : 5);
                shortGap = !shortGap;
            }
        }

        assertTrue(erratic.widening() > steady.widening(),
                "volatile refill timing should carry more margin: steady=" + steady.widening()
                        + " erratic=" + erratic.widening());
    }

    /** ...and it must cost nothing when timing is predictable, or it just wastes table size. */
    @Test
    void aPerfectlySteadyGapCarriesNoVolatilityMargin() {
        InFlightWindow window = window();
        run(window, 200, 120, 40_000, 2);   // gap of exactly 2 every time

        assertTrue(window.widening() < 3.0,
                "a steady gap of 2 should not inflate X much beyond 2, got " + window.widening());
    }

    @Test
    void wideningIsHeldWithinItsHardBounds() {
        InFlightWindow window = window();
        for (int cycle = 0; cycle < 400; cycle++) {
            window.observe(0, (long) cycle * INTERVAL * SECOND);
        }
        assertTrue(window.widening() <= InFlightWindow.MAX_WIDENING + 0.001,
                "never above the ceiling, got " + window.widening());
        assertTrue(window.widening() >= InFlightWindow.MIN_WIDENING,
                "never below the floor, got " + window.widening());
    }
}
