package com.palmyralabs.taskmesh.simple;
import com.palmyralabs.taskmesh.simple.entity.TmControlFlagEntity;
import com.palmyralabs.taskmesh.simple.repository.TmControlFlagRepository;

import com.palmyralabs.taskmesh.api.PauseController;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Cluster-flag style (matches {@code ClusterFlagPauseController} in
 * profiles.md), backed by a single {@code tm_control_flag} row. {@link #isPauseRequested()}
 * reads an in-memory cache refreshed on a poll interval — mirroring the
 * migration toolkit's {@code ControlFlagPoller} — rather than hitting the DB
 * on every call, since {@code TaskContext.isPauseRequested()} may be checked
 * frequently by processor code. {@link #requestPause()}/{@link #resume()}
 * write through immediately and update the cache locally.
 */
public class JpaPauseController implements PauseController {

    private final TmControlFlagRepository repository;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "taskmesh-jpa-control-flag-poller");
        thread.setDaemon(true);
        return thread;
    });

    private volatile boolean pauseRequested;

    public JpaPauseController(TmControlFlagRepository repository, Duration pollInterval) {
        this.repository = repository;
        this.pauseRequested = currentRow().isPauseRequested();
        long millis = pollInterval.toMillis();
        scheduler.scheduleWithFixedDelay(this::refresh, millis, millis, TimeUnit.MILLISECONDS);
    }

    private TmControlFlagEntity currentRow() {
        return repository.findById(1).orElseGet(() -> {
            TmControlFlagEntity entity = new TmControlFlagEntity();
            entity.setId(1);
            entity.setPauseRequested(false);
            return repository.save(entity);
        });
    }

    private void refresh() {
        pauseRequested = currentRow().isPauseRequested();
    }

    @Override
    public boolean isPauseRequested() {
        return pauseRequested;
    }

    @Override
    public void requestPause() {
        TmControlFlagEntity row = currentRow();
        row.setPauseRequested(true);
        repository.save(row);
        pauseRequested = true;
    }

    @Override
    public void resume() {
        TmControlFlagEntity row = currentRow();
        row.setPauseRequested(false);
        repository.save(row);
        pauseRequested = false;
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}
