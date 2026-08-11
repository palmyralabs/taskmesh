package com.palmyralabs.taskmesh.pro;

import java.time.Instant;

import org.springframework.scheduling.annotation.Scheduled;

import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.pro.entity.TmNodeEntity;
import com.palmyralabs.taskmesh.pro.repository.TmNodeRepository;

import lombok.RequiredArgsConstructor;

/**
 * Upserts this node's row in {@code tm_node} on a fixed interval — the
 * cluster's own record of which nodes exist and when each was last heard
 * from, independent of whether the node is actively claiming or executing
 * any task at that moment. Distinct from {@link
 * com.palmyralabs.taskmesh.api.HeartbeatStrategy}, which renews the lease on
 * one in-flight task; this is node presence, not task ownership.
 *
 * <p>{@code find-then-save} rather than a native upsert: this runs once per
 * {@code taskmesh.node-heartbeat-interval-minutes} (default 15) per node —
 * far too infrequent for the write-contention concerns that justify a native
 * {@code ON CONFLICT} elsewhere in this codebase (e.g. {@code
 * PartitionedTaskLedger.enqueue}). Spring Data's generated {@code save()} is
 * already transactional per call, so no explicit {@code @Transactional} here.
 */
@RequiredArgsConstructor
public class NodeHeartbeatWriter {

    private final TmNodeRepository repository;
    private final NodeInfoProvider nodeInfoProvider;

    @Scheduled(fixedDelayString = "#{${taskmesh.node-heartbeat-interval-minutes:15} * 60000}")
    public void heartbeat() {
        Instant now = Instant.now();
        String nodeId = nodeInfoProvider.nodeId();

        TmNodeEntity node = repository.findById(nodeId).orElseGet(() -> {
            TmNodeEntity created = new TmNodeEntity();
            created.setNodeId(nodeId);
            created.setFirstSeenAt(now);
            return created;
        });
        node.setLastHeartbeatAt(now);
        repository.save(node);
    }
}
