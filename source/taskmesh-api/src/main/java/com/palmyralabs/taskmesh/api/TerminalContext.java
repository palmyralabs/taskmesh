package com.palmyralabs.taskmesh.api;

import java.time.Instant;

/**
 * Handed to {@link TaskProcessor#onTerminal} at the moment a task reaches a
 * terminal state, from <b>inside the terminal transaction</b>.
 *
 * <p>Deliberately <b>not</b> {@link TaskContext}. That type's lifecycle methods
 * are meaningless or actively harmful here — {@code checkpoint()} would drive
 * lease renewal on a task that is ending, inside the transaction that is ending
 * it. Sharing one context type would turn a compile-time distinction into a
 * runtime one: whether a method is safe to call would depend on which callback
 * handed you the object.
 */
public interface TerminalContext extends TaskInfo {

    TerminalOutcome outcome();

    /** The failure's classification. {@code null} on {@link TerminalOutcome#COMPLETED}. */
    String reasonCode();

    /** The failure's summary. {@code null} on {@link TerminalOutcome#COMPLETED}. */
    String errorSummary();

    /** The node that drove the terminal transition. */
    String nodeId();

    /** The terminal transaction's own notion of "now", not a fresh clock read. */
    Instant terminalAt();
}
