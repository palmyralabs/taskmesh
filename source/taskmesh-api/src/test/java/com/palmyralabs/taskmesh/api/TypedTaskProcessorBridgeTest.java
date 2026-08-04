package com.palmyralabs.taskmesh.api;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TypedTaskProcessor} does the one unavoidable payload cast in three
 * places, and they are deliberately not all the same:
 *
 * <ul>
 *   <li>{@code process} rejects a null payload — an execution that cannot
 *       resolve its payload cannot do its work.</li>
 *   <li>{@code onStart} rejects it too, for the same reason: {@code process}
 *       is about to run and would reject it a moment later anyway.</li>
 *   <li>{@code onTerminal} <b>passes it through</b>. A task can go terminal
 *       with no execution behind it, and throwing there would roll back the
 *       terminal transition of a task that is otherwise fine — turning a
 *       missing payload into a task that can never finish.</li>
 * </ul>
 */
class TypedTaskProcessorBridgeTest {

    private record Order(String id) {
    }

    private static final class OrderProcessor extends TypedTaskProcessor<Order> {
        private final List<Order> startPayloads = new ArrayList<>();
        private final List<Order> terminalPayloads = new ArrayList<>();

        @Override
        public String taskType() {
            return "ORDER";
        }

        @Override
        protected Class<Order> payloadType() {
            return Order.class;
        }

        @Override
        protected void process(TaskContext ctx, Order payload) {
        }

        @Override
        protected void onStart(TaskContext ctx, Order payload) {
            startPayloads.add(payload);
        }

        @Override
        protected void onTerminal(TerminalContext ctx, Order payload) {
            terminalPayloads.add(payload);
        }
    }

    /** Counts payload reads, so the bridge can be held to loading it once. */
    private static final class StubTaskContext implements TaskContext {
        private final Object payload;
        private final AtomicInteger payloadReads = new AtomicInteger();

        private StubTaskContext(Object payload) {
            this.payload = payload;
        }

        @Override
        public TaskRef taskRef() {
            return new TaskRef("ORDER", "1");
        }

        @Override
        public int attemptCount() {
            return 1;
        }

        @Override
        public int retryCount() {
            return 0;
        }

        @Override
        public Object payload() {
            payloadReads.incrementAndGet();
            return payload;
        }

        @Override
        public void checkpoint() {
        }

        @Override
        public boolean isOwnershipValid() {
            return true;
        }

        @Override
        public boolean isPauseRequested() {
            return false;
        }

        @Override
        public boolean isShutdownRequested() {
            return false;
        }

        @Override
        public ProgressReporter progress() {
            return null;
        }

        @Override
        public ErrorLogger errorLogger() {
            return null;
        }
    }

    private static TerminalContext terminalContextWith(Object payload) {
        return new TerminalContext() {
            @Override
            public TaskRef taskRef() {
                return new TaskRef("ORDER", "1");
            }

            @Override
            public int attemptCount() {
                return 1;
            }

            @Override
            public int retryCount() {
                return 0;
            }

            @Override
            public Object payload() {
                return payload;
            }

            @Override
            public TerminalOutcome outcome() {
                return TerminalOutcome.COMPLETED;
            }

            @Override
            public String reasonCode() {
                return null;
            }

            @Override
            public String errorSummary() {
                return null;
            }

            @Override
            public String nodeId() {
                return "node-1";
            }

            @Override
            public Instant terminalAt() {
                return Instant.EPOCH;
            }
        };
    }

    // ---- onStart -------------------------------------------------------

    @Test
    void onStartCastsThePayloadAndReadsItExactlyOnce() {
        OrderProcessor processor = new OrderProcessor();
        Order order = new Order("abc");
        StubTaskContext ctx = new StubTaskContext(order);

        processor.onStart(ctx);

        assertEquals(List.of(order), processor.startPayloads);
        assertEquals(1, ctx.payloadReads.get(),
                "the bridge must not read the payload twice — process() reads the cached value next");
    }

    @Test
    void onStartRejectsANullPayloadJustAsProcessDoes() {
        OrderProcessor processor = new OrderProcessor();

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> processor.onStart(new StubTaskContext(null)));

        assertTrue(failure.getMessage().contains("ORDER"), failure.getMessage());
        assertTrue(failure.getMessage().contains("null"), failure.getMessage());
        assertEquals(List.of(), processor.startPayloads, "and never reaches the consumer's hook");
    }

    @Test
    void onStartRejectsAPayloadOfTheWrongType() {
        OrderProcessor processor = new OrderProcessor();

        assertThrows(IllegalStateException.class, () -> processor.onStart(new StubTaskContext("not an order")));
        assertEquals(List.of(), processor.startPayloads);
    }

    // ---- onTerminal ----------------------------------------------------

    @Test
    void onTerminalPassesANullPayloadThroughRatherThanRejectingIt() {
        OrderProcessor processor = new OrderProcessor();

        processor.onTerminal(terminalContextWith(null));

        assertEquals(1, processor.terminalPayloads.size(), "the callback must still run");
        assertNull(processor.terminalPayloads.get(0), "and see the null rather than an exception");
    }

    @Test
    void onTerminalCastsAPayloadOfTheRightTypeAsUsual() {
        OrderProcessor processor = new OrderProcessor();
        Order order = new Order("abc");

        processor.onTerminal(terminalContextWith(order));

        assertEquals(List.of(order), processor.terminalPayloads);
    }

    @Test
    void onTerminalStillRejectsAPayloadOfTheWrongTypeBecauseThatIsAProgrammingError() {
        OrderProcessor processor = new OrderProcessor();

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> processor.onTerminal(terminalContextWith("not an order")));

        assertTrue(failure.getMessage().contains("ORDER"), failure.getMessage());
        assertTrue(failure.getMessage().contains("java.lang.String"), failure.getMessage());
        assertEquals(List.of(), processor.terminalPayloads, "and never reaches the consumer's callback");
    }

    // ---- process -------------------------------------------------------

    @Test
    void processStillRejectsANullPayload() {
        OrderProcessor processor = new OrderProcessor();

        assertThrows(IllegalStateException.class, () -> processor.process(new StubTaskContext(null)));
    }
}
