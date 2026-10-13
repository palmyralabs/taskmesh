package com.palmyralabs.taskmesh.max;
import com.palmyralabs.taskmesh.max.entity.TmControlFlagEntity;
import com.palmyralabs.taskmesh.max.entity.TmNodeEntity;
import com.palmyralabs.taskmesh.max.repository.TmControlFlagRepository;
import com.palmyralabs.taskmesh.max.repository.TmNodeRepository;

import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.PauseController;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Combines two of the three independent control layers (see {@code
 * TaskmeshProperties.enabled} for the third, startup-time one): the
 * cluster-wide {@code tm_control_flag} single row, and this node's own {@code
 * tm_node.paused} column. {@link #isPauseRequested()} is true — this node
 * must not claim or execute anything — if <b>either</b> says so ("if anyone
 * failed, no task should run").
 *
 * <p>Both reads are an in-memory cache refreshed on a poll interval —
 * mirroring the migration toolkit's {@code ControlFlagPoller} — rather than
 * hitting the DB on every call, since {@code TaskContext.isPauseRequested()}
 * may be checked frequently by processor code. {@link #requestPause()}/
 * {@link #resume()} (cluster-wide only — per-node pause is written directly
 * by the consuming app for now, not through this interface, see
 * {@code NodeControlService}) write through immediately and update the cache
 * locally.
 *
 * <p>{@code nodeRepository} is nullable: a consumer without {@code tm_node}
 * in their schema gets cluster-only behavior, same as before this class
 * became node-aware.
 */
public class JpaPauseController implements PauseController {

    private final TmControlFlagRepository controlFlagRepository;
    private final TmNodeRepository nodeRepository;
    private final NodeInfoProvider nodeInfoProvider;
    private final Duration pollInterval;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "taskmesh-jpa-control-flag-poller");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile boolean clusterPauseRequested;
    private volatile boolean nodePauseRequested;

    public JpaPauseController(TmControlFlagRepository controlFlagRepository, TmNodeRepository nodeRepository,
            NodeInfoProvider nodeInfoProvider, Duration pollInterval) {
        this.controlFlagRepository = controlFlagRepository;
        this.nodeRepository = nodeRepository;
        this.nodeInfoProvider = nodeInfoProvider;
        this.pollInterval = pollInterval;
    }

    /**
     * Begins polling {@code tm_control_flag}/{@code tm_node} — deferred here
     * from the constructor so a node that never calls {@code
     * TaskMeshEngine.start()} ({@code taskmesh.enabled=false}) generates no
     * background DB traffic at all.
     */
    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        refresh();
        long millis = pollInterval.toMillis();
        scheduler.scheduleWithFixedDelay(this::refresh, millis, millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        scheduler.shutdownNow();
    }

    private TmControlFlagEntity currentControlFlagRow() {
        return controlFlagRepository.findById(1).orElseGet(() -> {
            TmControlFlagEntity entity = new TmControlFlagEntity();
            entity.setId(1);
            entity.setPauseRequested(false);
            return controlFlagRepository.save(entity);
        });
    }

    private void refresh() {
        clusterPauseRequested = currentControlFlagRow().isPauseRequested();
        nodePauseRequested = nodeRepository != null
                && nodeRepository.findById(nodeInfoProvider.nodeId()).map(TmNodeEntity::isPaused).orElse(false);
    }

    @Override
    public boolean isPauseRequested() {
        return clusterPauseRequested || nodePauseRequested;
    }

    @Override
    public void requestPause() {
        TmControlFlagEntity row = currentControlFlagRow();
        row.setPauseRequested(true);
        controlFlagRepository.save(row);
        clusterPauseRequested = true;
    }

    @Override
    public void resume() {
        TmControlFlagEntity row = currentControlFlagRow();
        row.setPauseRequested(false);
        controlFlagRepository.save(row);
        clusterPauseRequested = false;
    }
}
