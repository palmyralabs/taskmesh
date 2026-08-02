package com.palmyralabs.taskmesh.engine;

import com.palmyralabs.taskmesh.api.AuditEventSink;
import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.FailureClassifier;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.HistoryWriter;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.MetricsReporter;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.PauseController;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.ProcessorRegistry;
import com.palmyralabs.taskmesh.api.TaskLedger;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.api.TaskRef;
import com.palmyralabs.taskmesh.api.TimeoutPolicy;
import com.palmyralabs.taskmesh.engine.impl.DefaultFailureClassifier;
import com.palmyralabs.taskmesh.engine.impl.DefaultNodeInfoProvider;
import com.palmyralabs.taskmesh.engine.impl.DefaultProcessorRegistry;
import com.palmyralabs.taskmesh.engine.impl.FixedTimeoutPolicy;
import com.palmyralabs.taskmesh.engine.impl.SchedulingConfig;
import com.palmyralabs.taskmesh.engine.impl.TaskMeshEngineImpl;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * The driving implementation (D8/module-layout.md): feeder, claim loop,
 * dispatcher, worker pool. Wires the -api interfaces together; has no idea
 * what any TaskProcessor actually does, and never branches on deployment
 * topology (single-node vs multi-node is entirely a ClaimStrategy/
 * HeartbeatStrategy/TaskLedger implementation concern).
 *
 * <p>This is the public contract — construct via {@link #builder()}.
 * {@link com.palmyralabs.taskmesh.engine.impl.TaskMeshEngineImpl} and every
 * other concrete class live in the {@code impl} sub-package; nothing there
 * should be referenced directly.
 */
public interface TaskMeshEngine {

    void start();

    void stop();

    /** Operator-triggered pause — the one case AuditEventSink is written to (never for routine transitions). */
    void pause(String performedBy);

    void resume(String performedBy);

    /**
     * Plain enqueue — external API. Persists unconditionally; additionally
     * pushes into the execution queue if it isn't already past half
     * capacity (D25). If the gate fails, the row is still durable — the
     * engine's own poller picks it up on its next cycle.
     */
    void enqueue(TaskRef taskRef);

    /**
     * Priority entry point — external API. Persists (same durability
     * guarantee as {@link #enqueue}) and pushes directly into the execution
     * queue's priority lane for immediate pickup (D25).
     */
    void execute(TaskRef taskRef);

    static Builder builder() {
        return new Builder();
    }

    class Builder {
        private NodeInfoProvider nodeInfoProvider = new DefaultNodeInfoProvider();
        private List<TaskProcessor> processors;
        private ProcessorRegistry processorRegistry;
        private List<String> taskTypes;
        private ClaimStrategy claimStrategy;
        private HeartbeatStrategy heartbeatStrategy;
        private TaskLedger taskLedger;
        private FailureClassifier failureClassifier = new DefaultFailureClassifier();
        private PauseController pauseController;
        private HistoryWriter historyWriter;
        private AuditEventSink auditEventSink;
        private IncidentStore incidentStore;
        private MetricsReporter metricsReporter;
        private TimeoutPolicy timeoutPolicy = new FixedTimeoutPolicy(Duration.ofMinutes(5));
        private PayloadProviderFactory payloadProviderFactory;
        private SchedulingConfig schedulingConfig = SchedulingConfig.builder().build();

        /** Optional — defaults to {@link DefaultNodeInfoProvider} (hostname+PID), so most applications never need to call this. */
        public Builder nodeInfoProvider(NodeInfoProvider nodeInfoProvider) {
            this.nodeInfoProvider = nodeInfoProvider;
            return this;
        }

        public Builder processors(List<TaskProcessor> processors) {
            this.processors = processors;
            return this;
        }

        public Builder processorRegistry(ProcessorRegistry processorRegistry) {
            this.processorRegistry = processorRegistry;
            return this;
        }

        /** Only needed if processorRegistry() is used directly instead of processors() — otherwise derived automatically. */
        public Builder taskTypes(List<String> taskTypes) {
            this.taskTypes = taskTypes;
            return this;
        }

        public Builder claimStrategy(ClaimStrategy claimStrategy) {
            this.claimStrategy = claimStrategy;
            return this;
        }

        public Builder heartbeatStrategy(HeartbeatStrategy heartbeatStrategy) {
            this.heartbeatStrategy = heartbeatStrategy;
            return this;
        }

        public Builder taskLedger(TaskLedger taskLedger) {
            this.taskLedger = taskLedger;
            return this;
        }

        public Builder failureClassifier(FailureClassifier failureClassifier) {
            this.failureClassifier = failureClassifier;
            return this;
        }

        public Builder pauseController(PauseController pauseController) {
            this.pauseController = pauseController;
            return this;
        }

        public Builder historyWriter(HistoryWriter historyWriter) {
            this.historyWriter = historyWriter;
            return this;
        }

        public Builder auditEventSink(AuditEventSink auditEventSink) {
            this.auditEventSink = auditEventSink;
            return this;
        }

        public Builder incidentStore(IncidentStore incidentStore) {
            this.incidentStore = incidentStore;
            return this;
        }

        public Builder metricsReporter(MetricsReporter metricsReporter) {
            this.metricsReporter = metricsReporter;
            return this;
        }

        public Builder timeoutPolicy(TimeoutPolicy timeoutPolicy) {
            this.timeoutPolicy = timeoutPolicy;
            return this;
        }

        public Builder payloadProviderFactory(PayloadProviderFactory payloadProviderFactory) {
            this.payloadProviderFactory = payloadProviderFactory;
            return this;
        }

        public Builder schedulingConfig(SchedulingConfig schedulingConfig) {
            this.schedulingConfig = schedulingConfig;
            return this;
        }

        public TaskMeshEngine build() {
            Objects.requireNonNull(claimStrategy, "claimStrategy is required");
            Objects.requireNonNull(heartbeatStrategy, "heartbeatStrategy is required");
            Objects.requireNonNull(taskLedger, "taskLedger is required");
            Objects.requireNonNull(pauseController, "pauseController is required");
            Objects.requireNonNull(historyWriter, "historyWriter is required");
            Objects.requireNonNull(auditEventSink, "auditEventSink is required");
            Objects.requireNonNull(incidentStore, "incidentStore is required");
            Objects.requireNonNull(metricsReporter, "metricsReporter is required");
            Objects.requireNonNull(payloadProviderFactory, "payloadProviderFactory is required");
            if (processorRegistry == null) {
                Objects.requireNonNull(processors, "either processorRegistry or processors is required");
                processorRegistry = new DefaultProcessorRegistry(processors);
            }
            if (taskTypes == null) {
                Objects.requireNonNull(processors,
                        "taskTypes is required when processorRegistry() is supplied directly instead of processors()");
                taskTypes = processors.stream().map(TaskProcessor::taskType).toList();
            }
            return new TaskMeshEngineImpl(nodeInfoProvider, processorRegistry, taskTypes, claimStrategy, heartbeatStrategy,
                    taskLedger, failureClassifier, pauseController, historyWriter, auditEventSink, incidentStore,
                    metricsReporter, timeoutPolicy, payloadProviderFactory, schedulingConfig);
        }
    }
}
