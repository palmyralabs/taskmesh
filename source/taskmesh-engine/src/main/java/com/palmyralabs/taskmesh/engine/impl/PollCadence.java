package com.palmyralabs.taskmesh.engine.impl;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * Decides how often the poller should run, from the rate work is actually leaving the execution
 * queue. Pure policy: no threads, no scheduler, no clock of its own — the caller supplies the
 * consumption counter and the timestamp, so the whole control law is deterministic and testable
 * without running an engine.
 *
 * <h4>Why the cadence is not simply configured</h4>
 * A fixed interval has to be sized against {@code workerPoolSize / perExecutionTime}: too long
 * and workers idle in front of a full backlog, too short and it queries the database for nothing.
 * That ratio is a property of the consumer's workload, changes as the workload changes, and is
 * not knowable when the engine is built — the shipped 10s default against ~44ms executions left a
 * node running at roughly 5% of its worker capacity. Measuring beats guessing.
 *
 * <h4>The control law</h4>
 * Come back when {@code targetDrainFraction} of the regular lane will have drained at the
 * currently observed rate:
 *
 * <pre>
 *   delay = targetDrainFraction * regularCapacity / drainRate      (clamped to [min, max])
 * </pre>
 *
 * Half the lane by default: enough headroom for the poll itself to complete before workers run
 * dry, without refilling so often that each cycle returns a handful of rows.
 *
 * <p>The rate is an EWMA rather than the last sample. One quiet cycle — an empty backlog, a slow
 * query, a GC pause — must not collapse the cadence to its floor and turn the poller into a spin
 * loop, nor spike it to the ceiling and strand a queue that is about to be busy again.
 *
 * <p>Deliberately <b>not</b> an extension seam. D22 puts scheduling in the engine ("the engine
 * owns scheduling, calling {@code pullPending} on its own timer rather than each implementation
 * self-scheduling"), so this stays package-private engine internals rather than becoming another
 * interface a consumer has to understand.
 */
@Slf4j
final class PollCadence {

    /**
     * Hard bounds on the cadence and the page size. <b>Configuration narrows within these; it can
     * never escape them.</b> They exist because both knobs have a failure mode that is worse than
     * any tuning benefit: a sub-second cadence turns a deep backlog into a database spin loop, a
     * cadence beyond ten minutes makes a node look dead, a page below 16 spends more on round
     * trips than on work, and a page above 512 holds a long-running discovery query open while
     * claiming rows peers are already taking.
     */
    static final long MIN_POLL_MILLIS = 1_000;
    static final long MAX_POLL_MILLIS = 600_000;
    static final int MIN_PAGE_SIZE = 16;
    static final int MAX_PAGE_SIZE = 512;

    /**
     * How fast the cadence may <b>lengthen</b> — each idle cycle at most doubles the delay.
     *
     * <p>Deliberately asymmetric: slowing down is rate-limited, speeding up is not. The two
     * directions have very different costs. Backing off too eagerly is a latency bug — the first
     * idle cycle would jump straight to the ceiling and a node that then had work arrive would sit
     * on it for up to ten minutes. Speeding up too eagerly costs one cheap query. So an idle queue
     * eases off (1s, 2s, 4s, 8s... reaching a 30s ceiling after ~31s of genuine idleness), while
     * work appearing drops the cadence to the computed rate on the very next cycle.
     */
    private static final long BACKOFF_GROWTH = 2;

    private final long minDelayMillis;
    private final long maxDelayMillis;
    private final double targetDrainFraction;
    private final int regularCapacity;
    private final String nodeId;

    /** Smoothed tasks/second leaving the queue. */
    private double drainRatePerSecond;
    private long lastSampleNanos;
    private long lastSampleCount;
    private long currentDelayMillis;

    PollCadence(SchedulingConfig config, int regularCapacity, String nodeId) {
        long max = config.getPollMaxMillis() > 0
                ? config.getPollMaxMillis()
                : TimeUnit.SECONDS.toMillis(config.getQueryIntervalSeconds());
        // Configuration may narrow the window, never widen it past the hard bounds.
        this.minDelayMillis = clampMillis(config.getPollMinMillis());
        // A configured max below the floor would make the clamp order-dependent; the floor wins.
        this.maxDelayMillis = Math.max(this.minDelayMillis, clampMillis(max));
        double fraction = config.getPollTargetDrainFraction();
        this.targetDrainFraction = fraction > 0 && fraction <= 1 ? fraction : 0.5;
        this.regularCapacity = Math.max(1, regularCapacity);
        this.nodeId = nodeId;
        this.currentDelayMillis = this.minDelayMillis;
    }

    /** Establishes the baseline the first interval is measured against. */
    void reset(long consumed, long nanoTime) {
        this.lastSampleCount = consumed;
        this.lastSampleNanos = nanoTime;
    }

    /**
     * @param consumed total tasks taken off the queue by workers since the engine started
     * @param nanoTime {@code System.nanoTime()}, supplied so this stays deterministic in tests
     * @return milliseconds to wait before the next poll
     */
    long nextDelayMillis(long consumed, long nanoTime) {
        double elapsedSeconds = (nanoTime - lastSampleNanos) / 1_000_000_000.0;
        if (elapsedSeconds >= 0.001) {
            double observed = Math.max(0, consumed - lastSampleCount) / elapsedSeconds;
            drainRatePerSecond = drainRatePerSecond <= 0 ? observed : 0.7 * drainRatePerSecond + 0.3 * observed;
            lastSampleCount = consumed;
            lastSampleNanos = nanoTime;
        }

        long delay;
        if (drainRatePerSecond <= 0.5) {
            // Nothing is being consumed. Ease off rather than leaping to the ceiling: one quiet
            // cycle must not leave a node asleep for the full ceiling when work is about to
            // arrive. Reaching a 30s ceiling from 1s takes ~31s of continuous idleness.
            delay = Math.min(maxDelayMillis, currentDelayMillis * BACKOFF_GROWTH);
        } else {
            // Work is flowing: go straight to the rate the law asks for, undamped. Recovering
            // slowly would be the same latency bug in reverse.
            delay = (long) (1_000.0 * targetDrainFraction * regularCapacity / drainRatePerSecond);
        }
        delay = Math.max(minDelayMillis, Math.min(maxDelayMillis, delay));

        // Silence is a bug: throughput that changed because the cadence moved is undiagnosable
        // after the fact. Logged only on a material change, so a steady system stays quiet.
        // The comparison is >= deliberately: a backoff step is exactly BACKOFF_GROWTH, so a
        // strict > would make the entire idle ramp invisible — the one sequence most worth seeing.
        if (delay >= currentDelayMillis * 2 || delay * 2 <= currentDelayMillis) {
            log.info("taskmesh poll cadence {}ms -> {}ms on node {} (drain {} tasks/s, regular queue {})",
                    currentDelayMillis, delay, nodeId, Math.round(drainRatePerSecond), regularCapacity);
        }
        currentDelayMillis = delay;
        return delay;
    }

    /**
     * How many rows the next discovery query should ask for — the pagination size, bounded to
     * {@value #MIN_PAGE_SIZE}..{@value #MAX_PAGE_SIZE} whatever {@code batchSize} says.
     *
     * <p>Returns 0 when the queue has no room, so a full queue costs no query at all. Otherwise
     * it asks for at least the minimum page even if fewer slots are free: a handful of surplus
     * refs are simply refused by the non-blocking offer and picked up next cycle, which is
     * cheaper than a round trip that returns three rows.
     *
     * @param remainingCapacity free slots in the queue's regular lane
     * @param configuredBatchSize the consumer's {@code batchSize}, treated as a ceiling
     */
    int pageSize(int remainingCapacity, int configuredBatchSize) {
        if (remainingCapacity <= 0) {
            return 0;
        }
        int wanted = Math.min(remainingCapacity, Math.max(1, configuredBatchSize));
        return Math.max(MIN_PAGE_SIZE, Math.min(MAX_PAGE_SIZE, wanted));
    }

    private static long clampMillis(long millis) {
        return Math.max(MIN_POLL_MILLIS, Math.min(MAX_POLL_MILLIS, millis));
    }

    double drainRatePerSecond() {
        return drainRatePerSecond;
    }

    long currentDelayMillis() {
        return currentDelayMillis;
    }
}
