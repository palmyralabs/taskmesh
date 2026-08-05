package com.palmyralabs.taskmesh.max;

import lombok.extern.slf4j.Slf4j;

/**
 * Sizes {@link JpaBacklogPromoter}'s in-flight window from what the node is actually consuming,
 * instead of from configured watermarks somebody had to guess.
 *
 * <p>Pure policy: no database, no threads, no clock of its own — the caller supplies the observed
 * level and the timestamp, so the whole control law is deterministic and testable without an
 * engine. Same split as {@code PollCadence}.
 *
 * <h4>The law</h4>
 * <pre>
 *   x    = mean(last two consumption rates) * promotionIntervalSeconds   // rows consumed per cycle
 *   low  = 1.2 * x * X
 *   high = 1.5 * x * X
 * </pre>
 *
 * {@code x} is what the node eats in one promotion interval, so a window of {@code 1.2x..1.5x}
 * holds between one and one-and-a-half intervals of work — enough that a refill lands before the
 * node runs dry, and no more. Rate times interval rather than a raw per-cycle count: the two are
 * the same number today, but only the rate stays meaningful if the interval is retuned.
 *
 * <h4>X is measured, not controlled</h4>
 * Every node scans the same oldest unpromoted rows and exactly one wins the slice
 * ({@code stampPromoted}), so with N nodes a given node may only refill every Nth cycle while
 * holding just 1.5 cycles of work — and starve. X widens the band to cover that gap.
 *
 * <p>X is therefore <b>the observed number of cycles between this node's own successful refills</b>,
 * smoothed. That number is ≈ N under contention, which is what X is for, and it needs no cluster
 * membership to compute — taskmesh has none to ask, and a configured node count would go stale the
 * moment somebody scaled.
 *
 * <p>An earlier version drove X from a controller instead: widen when the level fell below
 * {@code low/2}, narrow when it sat above {@code low}. That oscillated between 1.6 and 5.4 in a
 * real 50k run, and the cause was structural rather than a matter of step sizes — {@code low} is
 * itself {@code 1.2 * x * X}, so widening raised the very threshold it was judged against, closing
 * a positive feedback loop. Measuring the gap breaks the loop: {@code refillGap} does not depend on
 * {@code low}, so X cannot chase its own tail, and the EWMA supplies the smoothing that step-size
 * tuning was failing to.
 *
 * <p>One controller-ish term survives, deliberately: if the level genuinely hits zero, the gap
 * measurement alone will not explain it (an undersized {@code backlogBatchSize} starves the window
 * no matter how often it refills), so a damped nudge widens X for that case too.
 */
@Slf4j
final class InFlightWindow {

    /** Fraction of x held before a refill triggers, and the level a refill fills to. */
    private static final double LOW_FACTOR = 1.2;
    private static final double HIGH_FACTOR = 1.5;

    /** X's starting point and its hard bounds — configuration cannot move these. */
    static final double INITIAL_WIDENING = 1.6;
    static final double MIN_WIDENING = 1.0;
    static final double MAX_WIDENING = 8.0;

    /** EWMA weight for the newest refill-gap sample. Low enough that one odd cycle barely moves X. */
    private static final double GAP_SMOOTHING = 0.2;
    /** Damped nudge applied when the window is actually empty, which the gap alone cannot explain. */
    private static final double STARVED_NUDGE = 0.15;
    /**
     * How much of the gap's volatility is carried as headroom. The window has to survive the worst
     * plausible gap, not the average one: if gaps alternate 1 and 5, the mean is 3 and a window
     * sized for 3 starves every time a 5 lands.
     *
     * <p>A quarter of a deviation, not a whole one. At 1.0 a measured 50k run held 8,337 rows
     * against 5,327 for the gap alone — 56% more table for no extra throughput, because the window
     * was never actually starving (observed levels stayed around 1,400-2,100, nowhere near zero).
     * The margin is insurance, and it should cost like insurance rather than like the risk.
     */
    private static final double VOLATILITY_FACTOR = 0.25;

    private final int floorRows;
    private final int ceilingRows;
    private final double intervalSeconds;
    private final String nodeId;

    private double widening = INITIAL_WIDENING;
    private double rateA = -1;
    private double rateB = -1;
    private long lastLevel = -1;
    private int lastPromoted;
    private long lastNanos;
    private int low;
    private int high;

    /** Cycles since this node last won a slice, and the smoothed history of that gap. */
    private int cyclesSinceRefill;
    private double refillGap = INITIAL_WIDENING;
    /** Smoothed |difference between the last two gaps| — how unpredictable refill timing is. */
    private double gapVolatility;
    private double previousGap = -1;
    private double loggedWidening = INITIAL_WIDENING;

    /**
     * @param configuredLow  {@code inFlightLowWatermark} — the floor the derived window may not go below,
     *                       and the seed until two consumption samples exist
     * @param configuredHigh {@code inFlightHighWatermark} — the ceiling the derived window may not exceed
     */
    InFlightWindow(int configuredLow, int configuredHigh, long promotionIntervalSeconds,
                    int regularQueueCapacity, String nodeId) {
        // The floor answers a different question from consumption. However slow the workload, the
        // poller asks for a full page each cycle (the queue's regular lane), and a node holding
        // less than that can never fill one — every discovery query comes back short. Two pages of
        // headroom, derived from the queue rather than guessed, so it tracks if the queue is
        // resized. A consumer may raise it, never lower it below this.
        int queueFloor = Math.max(1, 2 * regularQueueCapacity);
        this.floorRows = Math.max(queueFloor, Math.max(1, configuredLow));
        this.ceilingRows = Math.max(this.floorRows, configuredHigh);
        this.intervalSeconds = Math.max(1, promotionIntervalSeconds);
        this.nodeId = nodeId;
        this.low = this.floorRows;
        this.high = this.ceilingRows;
    }

    /**
     * Folds this cycle's observation in and re-derives the window.
     *
     * @param level    claimable rows this node can see right now
     * @param nanoTime {@code System.nanoTime()}, supplied so this stays deterministic in tests
     */
    void observe(long level, long nanoTime) {
        boolean warm = rateA >= 0 && rateB >= 0;
        if (lastLevel >= 0) {
            double elapsed = (nanoTime - lastNanos) / 1_000_000_000.0;
            if (elapsed >= 0.001) {
                // What left the window since the last look: where it was, plus what we added, minus
                // where it is now. Never negative — a peer stealing rows is not consumption.
                double consumed = Math.max(0, lastLevel + lastPromoted - level);
                pushRate(consumed / elapsed);
            }
        }
        // Count only cycles where this node actually WANTED a refill and did not get one — that is
        // contention. Counting every cycle conflates "I lost the slice" with "I was comfortable and
        // never asked", so a node sitting happily above its low mark for five cycles recorded a gap
        // of five, widened, raised its own low mark, needed refills even less often, and widened
        // further. Measured for real: gaps of 4-6 cycles with only three nodes, and an in-flight
        // table 58% larger than it needed to be.
        if (warm && level < low) {
            cyclesSinceRefill++;
        }
        lastLevel = level;
        lastPromoted = 0;
        lastNanos = nanoTime;

        // Only adapt once there is real history. Before that the level is legitimately 0 — nothing
        // has been promoted yet — and treating a cold start as starvation is what drove X from 1.6
        // to 5.4 in the first cycles of a real run.
        if (warm) {
            adaptWidening(level);
        }

        if (!warm) {
            low = floorRows;             // not enough history yet: fall back to what was configured
            high = ceilingRows;
            return;
        }
        double x = 0.5 * (rateA + rateB) * intervalSeconds;
        low = clamp(LOW_FACTOR * x * widening);
        // Derive high from the *clamped* low, so the hysteresis band survives the floor. Clamping
        // both independently collapsed them onto the floor whenever demand was lower than the
        // configured low, leaving low == high and no band at all.
        high = (int) Math.min(ceilingRows, Math.max(low, Math.round(low * (HIGH_FACTOR / LOW_FACTOR))));
    }

    /**
     * Tells the window how many rows the promoter actually added — both so consumption stays honest,
     * and because a non-zero refill is what closes a gap measurement.
     */
    void promoted(int rows) {
        lastPromoted += rows;
        if (rows > 0) {
            // The gap this node actually experienced between refills. Under slice contention with
            // N nodes this converges on N, which is exactly what the band has to cover.
            // cyclesSinceRefill counts only contended cycles (see observe), so a node that always
            // wins the slice it asks for records a gap of 1 and carries no widening at all.
            double gap = Math.max(1, cyclesSinceRefill);
            if (previousGap >= 0) {
                // The spread between consecutive gaps, smoothed. Unsigned deliberately: it only
                // ever adds headroom, so unlike a signed derivative it gives the window no
                // direction to hunt in, and gaps are small integers whose raw differences are far
                // too noisy to differentiate.
                gapVolatility = (1 - GAP_SMOOTHING) * gapVolatility
                        + GAP_SMOOTHING * Math.abs(gap - previousGap);
            }
            previousGap = gap;
            refillGap = (1 - GAP_SMOOTHING) * refillGap + GAP_SMOOTHING * gap;
            cyclesSinceRefill = 0;
        }
    }

    private void adaptWidening(long level) {
        // Mean gap plus one deviation of its spread. A steady cluster has volatility ~0, so this
        // costs nothing there and only buys margin when refill timing is genuinely erratic.
        double target = refillGap + VOLATILITY_FACTOR * gapVolatility;
        if (level <= 0) {
            // Empty window: the gap alone cannot explain this (an undersized batch starves it
            // however often it refills), so aim higher. Damped, not a 1.5x step.
            target = Math.max(target, widening * (1 + STARVED_NUDGE) / (1 - STARVED_NUDGE));
        }
        double next = Math.max(MIN_WIDENING, Math.min(MAX_WIDENING, target));
        // Move toward the target rather than jumping to it: smooth in both directions, with no
        // dependence on `low`, so there is no feedback path for X to oscillate along.
        widening = 0.85 * widening + 0.15 * next;

        // Silence is a bug: a window that quietly grew or shrank changes how much work exists in a
        // shared table. Compared against the last *logged* value so gradual drift still surfaces.
        if (widening >= loggedWidening * 1.5 || widening * 1.5 <= loggedWidening) {
            log.info("taskmesh in-flight widening {} -> {} on node {} (level {}, refill gap {} cycles, window {}..{})",
                    String.format("%.2f", loggedWidening), String.format("%.2f", widening),
                    nodeId, level, String.format("%.1f", refillGap), low, high);
            loggedWidening = widening;
        }
    }

    private void pushRate(double rate) {
        rateB = rateA;   // keep exactly the last two, as specified
        rateA = rate;
    }

    private int clamp(double rows) {
        return (int) Math.max(floorRows, Math.min(ceilingRows, Math.round(rows)));
    }

    int low() {
        return low;
    }

    int high() {
        return high;
    }

    double widening() {
        return widening;
    }
}
