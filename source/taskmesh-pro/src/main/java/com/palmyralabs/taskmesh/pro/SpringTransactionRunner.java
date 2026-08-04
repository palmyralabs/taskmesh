package com.palmyralabs.taskmesh.pro;

import com.palmyralabs.taskmesh.api.TransactionRunner;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.function.Supplier;

/**
 * This tier's {@link TransactionRunner} — a thin adapter over the same
 * {@link TransactionTemplate} the ledgers use, so work the engine wraps and
 * work the ledger does end up in one transaction rather than two.
 *
 * <p>The template's default propagation is {@code REQUIRED}, which is the whole
 * point: the ledger's own {@code transactionTemplate.execute(...)} joins the
 * boundary opened here instead of committing separately. The number of physical
 * transactions is unchanged; the outer one simply becomes the one that commits,
 * which is what lets {@code TaskProcessor.onTerminal} veto a terminal
 * transition by throwing.
 */
@RequiredArgsConstructor
public class SpringTransactionRunner implements TransactionRunner {

    private final TransactionTemplate transactionTemplate;

    @Override
    public <T> T call(Supplier<T> work) {
        return transactionTemplate.execute(status -> work.get());
    }
}
