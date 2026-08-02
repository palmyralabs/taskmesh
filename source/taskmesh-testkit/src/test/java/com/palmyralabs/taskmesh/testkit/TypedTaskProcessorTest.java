package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.TaskContext;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.api.TypedTaskProcessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypedTaskProcessorTest {

    record GreetingPayload(String name) {
    }

    static class GreetingProcessor extends TypedTaskProcessor<GreetingPayload> {
        String lastGreeting;

        @Override
        public String taskType() {
            return "GREETING";
        }

        @Override
        protected Class<GreetingPayload> payloadType() {
            return GreetingPayload.class;
        }

        @Override
        protected void process(TaskContext ctx, GreetingPayload payload) {
            ctx.checkpoint();
            lastGreeting = "Hello, " + payload.name();
            ctx.progress().report(1, 0);
        }
    }

    @Test
    void processesTypedPayload() throws Exception {
        GreetingProcessor processor = new GreetingProcessor();
        FakeTaskContext ctx = FakeTaskContext
                .builder(new TaskRef("GREETING", "t1"), new GreetingPayload("Ada"))
                .build();

        processor.process(ctx);

        assertEquals("Hello, Ada", processor.lastGreeting);
        assertEquals(1, ctx.checkpointCount());
        assertEquals(1, ctx.progressReporter().reports().size());
    }

    @Test
    void rejectsMismatchedPayloadTypeWithClearMessage() {
        GreetingProcessor processor = new GreetingProcessor();
        FakeTaskContext ctx = FakeTaskContext
                .builder(new TaskRef("GREETING", "t2"), 42)
                .build();

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> processor.process(ctx));

        assertTrue(ex.getMessage().contains("GreetingPayload"));
        assertTrue(ex.getMessage().contains("Integer"));
    }

    @Test
    void checkpointHookCanSimulateLostOwnershipMidExecution() throws Exception {
        FakeTaskContext ctx = FakeTaskContext
                .builder(new TaskRef("GREETING", "t3"), new GreetingPayload("Bob"))
                .onCheckpoint(() -> {
                })
                .build();

        assertTrue(ctx.isOwnershipValid());
        ctx.checkpoint();
        assertEquals(1, ctx.checkpointCount());

        ctx.loseOwnership();

        assertFalse(ctx.isOwnershipValid());
    }
}
