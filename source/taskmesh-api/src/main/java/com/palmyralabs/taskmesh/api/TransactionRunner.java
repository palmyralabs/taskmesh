package com.palmyralabs.taskmesh.api;

import java.util.function.Supplier;

/**
 * Lets the engine put a transaction boundary around work that spans a ledger
 * call and a consumer callback, without the engine itself knowing anything
 * about transactions — {@code taskmesh-engine} depends on {@code taskmesh-api}
 * and nothing else, and that stays true.
 *
 * <p>Used for one thing: {@link TaskProcessor#onTerminal} has to run inside the
 * same transaction as the terminal write the ledger performs, so a consumer's
 * projection and the queue's terminal state cannot diverge. The ledger opens
 * its own transaction internally; a tier's implementation of this interface is
 * expected to use the <b>same</b> transaction manager with join-if-present
 * semantics (Spring's default {@code PROPAGATION_REQUIRED}), so the ledger's
 * work joins the boundary opened here rather than committing separately. The
 * physical transaction count is unchanged — the outer one simply becomes the
 * one that commits.
 *
 * <p>When no implementation is wired the engine falls back to {@link #DIRECT},
 * which runs the work as-is. Everything still functions; the callback just
 * lands after the ledger has committed instead of inside its transaction, so
 * the no-divergence guarantee degrades to best-effort. That is the right
 * default for the in-memory testkit and for consumers with no transaction
 * manager to offer, and it is why this is optional rather than required.
 */
@FunctionalInterface
public interface TransactionRunner {

    /** No boundary at all — runs the work directly. */
    TransactionRunner DIRECT = new TransactionRunner() {
        @Override
        public <T> T call(Supplier<T> work) {
            return work.get();
        }
    };

    /**
     * Runs {@code work} inside a transaction, returning its result. An
     * exception thrown by {@code work} must mark the transaction for rollback
     * and propagate — the terminal callback relies on exactly that to be able
     * to veto a terminal transition.
     */
    <T> T call(Supplier<T> work);
}
