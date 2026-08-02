package com.palmyralabs.taskmesh.engine.impl;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Two-array priority execution queue (D25), pinned as a {@link java.util.concurrent.ThreadPoolExecutor}'s
 * work queue — priority-first draining lives entirely inside {@link #take()}/
 * {@link #poll(long, TimeUnit)}, which the pool already calls whenever a
 * worker goes idle, so no separate engine-side ordering logic is needed.
 *
 * <p>{@code priorityArray} — fixed capacity, small (5 by default, D25).
 * {@code regularArray} — the rest of total capacity. Both backed by
 * {@link LinkedBlockingQueue} rather than hand-rolled ring buffers (unlike
 * the {@code crownshaft} prior art this is modeled on) for correctness
 * under a real {@code ThreadPoolExecutor}'s concurrent access.
 *
 * <p>{@link #offerRegular}/{@link #offerFromPoller}/{@link #offerPriority}
 * are the differentiated push paths {@code enqueue}/{@code submit}/{@code execute}
 * use — the plain {@link #offer(Runnable)} required by {@code BlockingQueue}
 * always targets {@code regularArray}, for anything that submits through
 * the standard {@code Executor} API instead.
 */
class FifoPriorityQueueImpl extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {

    private static final long TAKE_POLL_INTERVAL_MILLIS = 20;

    private final int regularCapacity;
    private final LinkedBlockingQueue<Runnable> priorityArray;
    private final LinkedBlockingQueue<Runnable> regularArray;

    FifoPriorityQueueImpl(int priorityCapacity, int regularCapacity) {
        this.regularCapacity = regularCapacity;
        this.priorityArray = new LinkedBlockingQueue<>(priorityCapacity);
        this.regularArray = new LinkedBlockingQueue<>(regularCapacity);
    }

    /** crownshaft's {@code isHalfFull()} pattern (own prior art), applied to {@code regularArray}. */
    boolean isRegularHalfFull() {
        return regularArray.size() > (regularCapacity >> 1);
    }

    /** {@code enqueue()}'s push — non-blocking; caller checks {@link #isRegularHalfFull()} first. */
    boolean offerRegular(Runnable task) {
        return regularArray.offer(task);
    }

    /** {@code submit}'s (poller) push — unconditional, no admission gate. */
    boolean offerFromPoller(Runnable task) {
        return regularArray.offer(task);
    }

    /** {@code execute()}'s push — blocks up to the given timeout, then rejects. */
    boolean offerPriority(Runnable task, long timeout, TimeUnit unit) throws InterruptedException {
        return priorityArray.offer(task, timeout, unit);
    }

    @Override
    public Runnable take() throws InterruptedException {
        Runnable task = priorityArray.poll();
        if (task != null) {
            return task;
        }
        // Poll regularArray in short slices rather than a single indefinite take()
        // so a priority item arriving while every worker is parked here still
        // gets noticed within one interval instead of waiting for regular work too.
        while (true) {
            task = regularArray.poll(TAKE_POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            if (task != null) {
                return task;
            }
            task = priorityArray.poll();
            if (task != null) {
                return task;
            }
        }
    }

    @Override
    public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
        Runnable task = priorityArray.poll();
        if (task != null) {
            return task;
        }
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return null;
            }
            long sliceMillis = Math.min(TAKE_POLL_INTERVAL_MILLIS, TimeUnit.NANOSECONDS.toMillis(remaining) + 1);
            task = regularArray.poll(sliceMillis, TimeUnit.MILLISECONDS);
            if (task != null) {
                return task;
            }
            task = priorityArray.poll();
            if (task != null) {
                return task;
            }
        }
    }

    @Override
    public Runnable poll() {
        Runnable task = priorityArray.poll();
        return task != null ? task : regularArray.poll();
    }

    @Override
    public Runnable peek() {
        Runnable task = priorityArray.peek();
        return task != null ? task : regularArray.peek();
    }

    @Override
    public boolean offer(Runnable task) {
        return offerRegular(task);
    }

    @Override
    public void put(Runnable task) throws InterruptedException {
        regularArray.put(task);
    }

    @Override
    public boolean offer(Runnable task, long timeout, TimeUnit unit) throws InterruptedException {
        return regularArray.offer(task, timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return priorityArray.remainingCapacity() + regularArray.remainingCapacity();
    }

    /** Room left in regularArray specifically — the poller uses this to size its pull. */
    int regularRemainingCapacity() {
        return regularArray.remainingCapacity();
    }

    @Override
    public int size() {
        return priorityArray.size() + regularArray.size();
    }

    @Override
    public int drainTo(Collection<? super Runnable> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super Runnable> c, int maxElements) {
        int drained = priorityArray.drainTo(c, maxElements);
        if (drained < maxElements) {
            drained += regularArray.drainTo(c, maxElements - drained);
        }
        return drained;
    }

    @Override
    public Iterator<Runnable> iterator() {
        return new CompositeIterator();
    }

    private final class CompositeIterator implements Iterator<Runnable> {
        private final Iterator<Runnable> priorityIterator = priorityArray.iterator();
        private final Iterator<Runnable> regularIterator = regularArray.iterator();
        private boolean lastFromPriority;
        private boolean lastReturned;

        @Override
        public boolean hasNext() {
            return priorityIterator.hasNext() || regularIterator.hasNext();
        }

        @Override
        public Runnable next() {
            lastReturned = true;
            if (priorityIterator.hasNext()) {
                lastFromPriority = true;
                return priorityIterator.next();
            }
            if (regularIterator.hasNext()) {
                lastFromPriority = false;
                return regularIterator.next();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (!lastReturned) {
                throw new IllegalStateException();
            }
            lastReturned = false;
            if (lastFromPriority) {
                priorityIterator.remove();
            } else {
                regularIterator.remove();
            }
        }
    }
}
