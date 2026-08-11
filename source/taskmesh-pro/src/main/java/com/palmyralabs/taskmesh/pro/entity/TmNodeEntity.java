package com.palmyralabs.taskmesh.pro.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * One row per node that has ever heartbeat into this cluster — the roster,
 * not a hot table. Deliberately separate from {@code tm_in_flight.node_id}
 * (which only ever names a node while it holds a claim): this table answers
 * "which nodes exist and when did we last hear from them" independent of
 * whether any node is actively claiming or executing a task right now.
 */
@Entity
@Table(name = "tm_node")
@Getter
@Setter
@NoArgsConstructor
public class TmNodeEntity {

    @Id
    @Column(name = "node_id")
    private String nodeId;

    @Column(name = "first_seen_at", nullable = false)
    private Instant firstSeenAt;

    @Column(name = "last_heartbeat_at", nullable = false)
    private Instant lastHeartbeatAt;
}
