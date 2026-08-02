package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.ProgressReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link #heartbeat()} is a convenience alias for {@link DefaultTaskContext#checkpoint()}
 * — for processors that report progress at a finer granularity than they
 * explicitly checkpoint. {@link #report} has no persistent sink yet (no
 * infra interface covers sub-task item counts); logged only. See the
 * ProgressReporter/TaskContext.checkpoint() relationship note in
 * docs/planning/open-questions.md's research (Q-OBSERVABILITY territory).
 */
class DefaultProgressReporter implements ProgressReporter {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultProgressReporter.class);

    private final DefaultTaskContext ctx;

    DefaultProgressReporter(DefaultTaskContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void report(long itemsProcessed, long itemsFailed) {
        LOG.debug("{} progress: processed={} failed={}", ctx.taskRef(), itemsProcessed, itemsFailed);
    }

    @Override
    public void heartbeat() {
        ctx.checkpoint();
    }
}
