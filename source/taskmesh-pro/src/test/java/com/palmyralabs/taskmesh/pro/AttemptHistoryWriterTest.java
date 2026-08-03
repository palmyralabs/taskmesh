package com.palmyralabs.taskmesh.pro;
import com.palmyralabs.taskmesh.pro.entity.TmAttemptHistoryEntity;
import com.palmyralabs.taskmesh.pro.entity.TmAttemptHistoryId;
import com.palmyralabs.taskmesh.pro.repository.TmAttemptHistoryRepository;

import com.palmyralabs.taskmesh.api.HistoryEvent;
import com.palmyralabs.taskmesh.api.HistoryEventType;
import com.palmyralabs.taskmesh.api.TaskRef;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** BATCH profile's coarse HistoryWriter — one row per attempt, INSERT-then-UPDATE (D28/infra-implementations.md Variant A). */
@DataJpaTest
class AttemptHistoryWriterTest {

    @Autowired
    private TmAttemptHistoryRepository repository;

    @Test
    void oneRowPerAttemptUpdatedInPlaceThroughToCompletion() {
        AttemptHistoryWriter writer = new AttemptHistoryWriter(repository);
        TaskRef ref = new TaskRef("T", "attempt-1");

        writer.record(new HistoryEvent(ref, 1, HistoryEventType.STARTED, null, Instant.now()));
        TmAttemptHistoryId id = new TmAttemptHistoryId("T", "attempt-1", 1);
        TmAttemptHistoryEntity afterStart = repository.findById(id).orElseThrow();
        assertEquals(HistoryEventType.STARTED, afterStart.getStatus());
        assertNotNull(afterStart.getStartedAt());
        assertNull(afterStart.getEndedAt());

        writer.record(new HistoryEvent(ref, 1, HistoryEventType.HEARTBEAT, "still going", Instant.now()));
        TmAttemptHistoryEntity afterHeartbeat = repository.findById(id).orElseThrow();
        assertEquals(HistoryEventType.HEARTBEAT, afterHeartbeat.getStatus(), "non-terminal events still update the same row");
        assertNull(afterHeartbeat.getEndedAt(), "non-terminal events must not close the row");

        writer.record(new HistoryEvent(ref, 1, HistoryEventType.COMPLETED, null, Instant.now()));
        TmAttemptHistoryEntity afterComplete = repository.findById(id).orElseThrow();
        assertEquals(HistoryEventType.COMPLETED, afterComplete.getStatus());
        assertNotNull(afterComplete.getEndedAt());

        assertEquals(1, repository.count(), "must stay exactly one row for this attempt, never a second insert");
    }

    @Test
    void terminalEventWithNoPriorStartedStillCreatesARow() {
        AttemptHistoryWriter writer = new AttemptHistoryWriter(repository);
        TaskRef ref = new TaskRef("T", "no-processor");

        writer.record(new HistoryEvent(ref, 1, HistoryEventType.FAILED, "NO_PROCESSOR", Instant.now()));

        TmAttemptHistoryEntity row = repository.findById(new TmAttemptHistoryId("T", "no-processor", 1)).orElseThrow();
        assertEquals(HistoryEventType.FAILED, row.getStatus());
        assertNotNull(row.getEndedAt());
    }
}
