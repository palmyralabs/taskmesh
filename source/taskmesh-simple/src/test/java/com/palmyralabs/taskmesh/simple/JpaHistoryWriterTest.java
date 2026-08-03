package com.palmyralabs.taskmesh.simple;
import com.palmyralabs.taskmesh.simple.repository.TmHistoryEventRepository;

import com.palmyralabs.taskmesh.api.HistoryEvent;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Locks in HistoryLevel's filtering — the BATCH/HIGH_VOLUME/REACTIVE differentiator (D28, TaskmeshProfile). */
@DataJpaTest
class JpaHistoryWriterTest {

    @Autowired
    private TmHistoryEventRepository repository;

    private long countFor(String taskId) {
        return repository.findAll().stream().filter(e -> e.getTaskId().equals(taskId)).count();
    }

    @Test
    void minimalLevelOnlyRecordsTerminalOutcomes() {
        JpaHistoryWriter writer = new JpaHistoryWriter(repository, HistoryLevel.MINIMAL);
        TaskRef ref = new TaskRef("T", "minimal-1");

        writer.record(new HistoryEvent(ref, 1, HistoryEventType.STARTED, null, Instant.now(), "node-1"));
        writer.record(new HistoryEvent(ref, 1, HistoryEventType.HEARTBEAT, null, Instant.now(), "node-1"));
        writer.record(new HistoryEvent(ref, 1, HistoryEventType.COMPLETED, null, Instant.now(), "node-1"));

        assertEquals(1, countFor("minimal-1"), "MINIMAL should only keep the COMPLETED event");
    }

    @Test
    void standardLevelSkipsHeartbeat() {
        JpaHistoryWriter writer = new JpaHistoryWriter(repository, HistoryLevel.STANDARD);
        TaskRef ref = new TaskRef("T", "standard-1");

        writer.record(new HistoryEvent(ref, 1, HistoryEventType.STARTED, null, Instant.now(), "node-1"));
        writer.record(new HistoryEvent(ref, 1, HistoryEventType.HEARTBEAT, null, Instant.now(), "node-1"));
        writer.record(new HistoryEvent(ref, 1, HistoryEventType.COMPLETED, null, Instant.now(), "node-1"));

        assertEquals(2, countFor("standard-1"), "STANDARD should keep STARTED + COMPLETED but skip HEARTBEAT");
    }

    @Test
    void verboseLevelRecordsEverythingIncludingHeartbeat() {
        JpaHistoryWriter writer = new JpaHistoryWriter(repository, HistoryLevel.VERBOSE);
        TaskRef ref = new TaskRef("T", "verbose-1");

        writer.record(new HistoryEvent(ref, 1, HistoryEventType.STARTED, null, Instant.now(), "node-1"));
        writer.record(new HistoryEvent(ref, 1, HistoryEventType.HEARTBEAT, null, Instant.now(), "node-1"));
        writer.record(new HistoryEvent(ref, 1, HistoryEventType.COMPLETED, null, Instant.now(), "node-1"));

        assertEquals(3, countFor("verbose-1"));
    }

    @Test
    void defaultConstructorMatchesVerbose() {
        JpaHistoryWriter writer = new JpaHistoryWriter(repository);
        TaskRef ref = new TaskRef("T", "default-1");

        writer.record(new HistoryEvent(ref, 1, HistoryEventType.HEARTBEAT, null, Instant.now(), "node-1"));

        assertEquals(1, countFor("default-1"), "no-arg constructor must preserve prior VERBOSE-equivalent behavior");
    }
}
