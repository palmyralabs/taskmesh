package com.palmyralabs.taskmesh.api;

public interface TaskProcessor {

    String taskType();

    void process(TaskContext ctx) throws Exception;

    /**
     * Fired at the start of every execution, just before {@link #process}. Default
     * no-op, so processors that do not need it are unaffected.
     *
     * <p>There is deliberately no separate "re-run" callback: the D33 counter pair
     * already says which kind of start this is, and a consumer reads it off
     * {@link TaskInfo}:
     *
     * <ul>
     *   <li>{@code attemptCount == 1, retryCount == 0} — the task's first ever
     *       execution.</li>
     *   <li>{@code attemptCount > 1, retryCount == 0} — <b>an operator re-ran it</b>.
     *       A consumer holding a derived projection uses this to move that row back
     *       off its terminal state; nothing else in the engine will tell it that the
     *       task is alive again.</li>
     *   <li>{@code retryCount > 0} — an automatic retry within the current attempt,
     *       or a reclaim after the previous execution's node died.</li>
     * </ul>
     *
     * <p>That last distinction is what keeps the write volume this callback implies
     * affordable. A consumer maintaining a large projection can write only when
     * {@code retryCount == 0} — one row-touch per attempt, matching the one
     * {@link #onTerminal} does — and ignore retries entirely.
     *
     * <h4>Contract</h4>
     * Runs in its own short transaction, so a projection write here commits with
     * nothing else attached. Throwing aborts the execution before {@code process}
     * runs, and the failure travels the ordinary classified-failure path: it is
     * retried, it consumes budget, and it eventually fails the task terminally
     * rather than looping forever. Unlike {@link #onTerminal} this is not the last
     * word on the task — a start that never happened is self-correcting, because
     * the next execution fires this again.
     */
    default void onStart(TaskContext ctx) {
    }

    /**
     * Fired <b>once per terminal transition</b>, at the moment the task reaches
     * {@code COMPLETED} or {@code FAILED} — never on a within-budget retry, a
     * yield, an ordinary reclaim, or a release that lost ownership. Default
     * no-op, so processors that do not need it are unaffected.
     *
     * <p>Exists because a processor cannot work terminality out for itself: it
     * knows an execution failed, but whether the retry budget is now spent is
     * decided inside {@code release()}/{@code claim()}, after the processor has
     * already thrown. A processor writing "failed" from its own catch block
     * would mark tasks terminal that are merely mid-retry.
     *
     * <h4>Contract — read before implementing</h4>
     * This runs <b>inside the engine's terminal transaction</b>, holding row
     * locks on the hot claim table. That is deliberate: a consumer's derived
     * projection and the queue's terminal state must not be able to diverge,
     * and there is no reconciliation job that could tell you how far apart they
     * had drifted. It also constrains what you may do here:
     *
     * <ul>
     *   <li><b>Same datasource only.</b> Joining the engine's transaction is the
     *       point; a second datasource gets you a distributed-transaction
     *       problem instead of a guarantee.</li>
     *   <li><b>No external I/O, no long-running work.</b> You are holding locks
     *       that every other node's claim query contends on.</li>
     *   <li><b>Be idempotent.</b> This can fire more than once for the same
     *       terminal transition (see below).</li>
     *   <li><b>Throwing means "do not let this task go terminal".</b> The
     *       exception rolls the terminal transition back: the task stays
     *       claimed, its lease lapses, and it is reclaimed and re-executed. For
     *       an idempotent task that is wasted work rather than corruption — but
     *       it means <em>a broken callback presents as tasks that never
     *       finish</em>, which is worth knowing before you debug one.</li>
     * </ul>
     *
     * <p>{@link TerminalContext#payload()} may be {@code null} — a terminal
     * reached without a preceding execution (an exhausted budget noticed at
     * claim time, say) has no payload loaded, and the engine does not fetch one
     * just to report the outcome.
     */
    default void onTerminal(TerminalContext ctx) {
    }
}
