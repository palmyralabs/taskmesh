package com.palmyralabs.taskmesh.api;

/**
 * Two callers raced to enqueue the same brand-new {@link TaskRef} and this one
 * lost: the other has already created the task's rows.
 *
 * <p>Not an error condition. {@code enqueue} is documented as idempotent, so the
 * winner's rows are exactly the outcome this call wanted too — the engine
 * catches this and reports success. It exists as a typed exception only because
 * the losing insert has to abort its transaction, and the engine
 * ({@code taskmesh-api} and nothing else on its classpath) cannot recognise a
 * persistence-provider exception to tell that abort apart from a real failure.
 */
public class DuplicateEnqueueException extends RuntimeException {

    private final transient TaskRef taskRef;

    public DuplicateEnqueueException(TaskRef taskRef, Throwable cause) {
        super("Lost a concurrent enqueue race for " + taskRef + " — the other caller's rows stand", cause);
        this.taskRef = taskRef;
    }

    public TaskRef taskRef() {
        return taskRef;
    }
}
