package com.palmyralabs.taskmesh.max.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * JPA composite-key class for {@link TmIncidentEntity} — {@code id} stays
 * globally unique (a shared sequence, not per-partition identity);
 * {@code partitionKey} is required alongside it purely to satisfy Postgres's
 * requirement that the partition column appear in every unique constraint.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TmIncidentId implements Serializable {
    private Long id;
    private String partitionKey;
}
