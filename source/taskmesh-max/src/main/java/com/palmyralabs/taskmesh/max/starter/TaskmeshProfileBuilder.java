package com.palmyralabs.taskmesh.max.starter;

import com.palmyralabs.taskmesh.max.SpringTransactionRunner;
import com.palmyralabs.taskmesh.api.AuditEventSink;
import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.HistoryWriter;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.MetricsReporter;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.PauseController;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.api.TaskLedger;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.engine.impl.DefaultNodeInfoProvider;
import com.palmyralabs.taskmesh.engine.impl.SchedulingConfig;
import com.palmyralabs.taskmesh.max.ExponentialBackoffRetryPolicy;
import com.palmyralabs.taskmesh.max.JpaAuditEventSink;
import com.palmyralabs.taskmesh.max.JpaIncidentStore;
import com.palmyralabs.taskmesh.max.JpaPauseController;
import com.palmyralabs.taskmesh.max.NoopBacklogPromoter;
import com.palmyralabs.taskmesh.max.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.max.repository.TmAuditEventRepository;
import com.palmyralabs.taskmesh.max.repository.TmControlFlagRepository;
import com.palmyralabs.taskmesh.max.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;
import com.palmyralabs.taskmesh.starter.Slf4jMetricsReporter;
import com.palmyralabs.taskmesh.starter.TaskmeshProperties;
import com.palmyralabs.taskmesh.starter.WorkloadProfile;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Non-Spring-Boot equivalent of {@link TaskmeshEngineAutoConfiguration} —
 * for a consumer wiring the taskmesh-max tier manually (plain DI
 * container, or no DI container at all). Same {@link ProfileFactories}
 * mapping, same override points, just via setters instead of
 * {@code @ConditionalOnMissingBean} beans.
 */
public final class TaskmeshProfileBuilder {

    private final WorkloadProfile profile;
    private TaskmeshProperties properties = new TaskmeshProperties();

    private TmTaskRepository taskRepository;
    private TmInFlightRepository inFlightRepository;
    private TmHistoryEventRepository historyEventRepository;
    private TmAttemptHistoryRepository attemptHistoryRepository;
    private TmAuditEventRepository auditEventRepository;
    private TmIncidentRepository incidentRepository;
    private TmControlFlagRepository controlFlagRepository;
    private PlatformTransactionManager transactionManager;
    private List<TaskProcessor> processors;
    private PayloadProviderFactory payloadProviderFactory;

    private NodeInfoProvider nodeInfoProvider;
    private RetryPolicy retryPolicy;
    private MetricsReporter metricsReporter;
    private ClaimStrategy claimStrategyOverride;
    private HeartbeatStrategy heartbeatStrategyOverride;
    private HistoryWriter historyWriterOverride;
    private PauseController pauseControllerOverride;
    private AuditEventSink auditEventSinkOverride;
    private IncidentStore incidentStoreOverride;
    private BacklogPromoter backlogPromoterOverride;

    private TaskmeshProfileBuilder(WorkloadProfile profile) {
        this.profile = profile;
    }

    public static TaskmeshProfileBuilder profile(WorkloadProfile profile) {
        return new TaskmeshProfileBuilder(profile);
    }

    public TaskmeshProfileBuilder properties(TaskmeshProperties properties) {
        this.properties = properties;
        return this;
    }

    public TaskmeshProfileBuilder taskRepository(TmTaskRepository taskRepository) {
        this.taskRepository = taskRepository;
        return this;
    }

    public TaskmeshProfileBuilder inFlightRepository(TmInFlightRepository inFlightRepository) {
        this.inFlightRepository = inFlightRepository;
        return this;
    }

    public TaskmeshProfileBuilder historyEventRepository(TmHistoryEventRepository historyEventRepository) {
        this.historyEventRepository = historyEventRepository;
        return this;
    }

    public TaskmeshProfileBuilder attemptHistoryRepository(TmAttemptHistoryRepository attemptHistoryRepository) {
        this.attemptHistoryRepository = attemptHistoryRepository;
        return this;
    }

    public TaskmeshProfileBuilder auditEventRepository(TmAuditEventRepository auditEventRepository) {
        this.auditEventRepository = auditEventRepository;
        return this;
    }

    public TaskmeshProfileBuilder incidentRepository(TmIncidentRepository incidentRepository) {
        this.incidentRepository = incidentRepository;
        return this;
    }

    public TaskmeshProfileBuilder controlFlagRepository(TmControlFlagRepository controlFlagRepository) {
        this.controlFlagRepository = controlFlagRepository;
        return this;
    }

    public TaskmeshProfileBuilder transactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        return this;
    }

    public TaskmeshProfileBuilder processors(List<TaskProcessor> processors) {
        this.processors = processors;
        return this;
    }

    public TaskmeshProfileBuilder payloadProviderFactory(PayloadProviderFactory payloadProviderFactory) {
        this.payloadProviderFactory = payloadProviderFactory;
        return this;
    }

    public TaskmeshProfileBuilder nodeInfoProvider(NodeInfoProvider nodeInfoProvider) {
        this.nodeInfoProvider = nodeInfoProvider;
        return this;
    }

    public TaskmeshProfileBuilder retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public TaskmeshProfileBuilder metricsReporter(MetricsReporter metricsReporter) {
        this.metricsReporter = metricsReporter;
        return this;
    }

    public TaskmeshProfileBuilder claimStrategy(ClaimStrategy claimStrategy) {
        this.claimStrategyOverride = claimStrategy;
        return this;
    }

    public TaskmeshProfileBuilder heartbeatStrategy(HeartbeatStrategy heartbeatStrategy) {
        this.heartbeatStrategyOverride = heartbeatStrategy;
        return this;
    }

    public TaskmeshProfileBuilder historyWriter(HistoryWriter historyWriter) {
        this.historyWriterOverride = historyWriter;
        return this;
    }

    public TaskmeshProfileBuilder pauseController(PauseController pauseController) {
        this.pauseControllerOverride = pauseController;
        return this;
    }

    public TaskmeshProfileBuilder auditEventSink(AuditEventSink auditEventSink) {
        this.auditEventSinkOverride = auditEventSink;
        return this;
    }

    public TaskmeshProfileBuilder incidentStore(IncidentStore incidentStore) {
        this.incidentStoreOverride = incidentStore;
        return this;
    }

    public TaskmeshProfileBuilder backlogPromoter(BacklogPromoter backlogPromoter) {
        this.backlogPromoterOverride = backlogPromoter;
        return this;
    }

    public TaskMeshEngine build() {
        Objects.requireNonNull(taskRepository, "taskRepository is required");
        Objects.requireNonNull(inFlightRepository, "inFlightRepository is required");
        Objects.requireNonNull(historyEventRepository, "historyEventRepository is required");
        Objects.requireNonNull(attemptHistoryRepository, "attemptHistoryRepository is required");
        Objects.requireNonNull(auditEventRepository, "auditEventRepository is required");
        Objects.requireNonNull(incidentRepository, "incidentRepository is required");
        Objects.requireNonNull(controlFlagRepository, "controlFlagRepository is required");
        Objects.requireNonNull(transactionManager, "transactionManager is required");
        Objects.requireNonNull(processors, "processors is required");
        Objects.requireNonNull(payloadProviderFactory, "payloadProviderFactory is required");

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        NodeInfoProvider resolvedNodeInfoProvider = nodeInfoProvider != null ? nodeInfoProvider
                : properties.getNodeId().isBlank() ? new DefaultNodeInfoProvider() : properties::getNodeId;
        RetryPolicy resolvedRetryPolicy = retryPolicy != null ? retryPolicy
                : new ExponentialBackoffRetryPolicy(properties.getMaxRetries(),
                        Duration.ofMinutes(properties.getBackoffBaseMinutes()), Duration.ofMinutes(properties.getBackoffCapMinutes()),
                        properties.getBackoffJitter());
        MetricsReporter resolvedMetricsReporter = metricsReporter != null ? metricsReporter : new Slf4jMetricsReporter();
        IncidentStore resolvedIncidentStore = incidentStoreOverride != null ? incidentStoreOverride
                : new JpaIncidentStore(incidentRepository);
        AuditEventSink resolvedAuditEventSink = auditEventSinkOverride != null ? auditEventSinkOverride
                : new JpaAuditEventSink(auditEventRepository);
        PauseController resolvedPauseController = pauseControllerOverride != null ? pauseControllerOverride
                : new JpaPauseController(controlFlagRepository, Duration.ofSeconds(properties.getQueryIntervalSeconds()));
        ClaimStrategy resolvedClaimStrategy = claimStrategyOverride != null ? claimStrategyOverride
                : ProfileFactories.claimStrategy(properties, taskRepository, inFlightRepository,
                        transactionTemplate, resolvedNodeInfoProvider, resolvedRetryPolicy);
        TaskLedger resolvedTaskLedger = (TaskLedger) resolvedClaimStrategy;
        // After the ledger, not before: THROTTLED promotion measures its in-flight window through
        // the very same ledger instance the engine claims through.
        BacklogPromoter resolvedBacklogPromoter = backlogPromoterOverride != null ? backlogPromoterOverride
                : ProfileFactories.backlogPromoter(properties, taskRepository, inFlightRepository,
                        transactionTemplate, resolvedClaimStrategy, resolvedNodeInfoProvider);
        HeartbeatStrategy resolvedHeartbeatStrategy = heartbeatStrategyOverride != null ? heartbeatStrategyOverride
                : ProfileFactories.heartbeatStrategy(profile, properties, resolvedClaimStrategy, resolvedIncidentStore);
        HistoryWriter resolvedHistoryWriter = historyWriterOverride != null ? historyWriterOverride
                : ProfileFactories.historyWriter(profile, historyEventRepository, attemptHistoryRepository);

        TaskMeshEngine engine = TaskMeshEngine.builder()
                .transactionRunner(new SpringTransactionRunner(transactionTemplate))
                .nodeInfoProvider(resolvedNodeInfoProvider)
                .processors(processors)
                .claimStrategy(resolvedClaimStrategy)
                .taskLedger(resolvedTaskLedger)
                .backlogPromoter(resolvedBacklogPromoter)
                .heartbeatStrategy(resolvedHeartbeatStrategy)
                .pauseController(resolvedPauseController)
                .historyWriter(resolvedHistoryWriter)
                .auditEventSink(resolvedAuditEventSink)
                .incidentStore(resolvedIncidentStore)
                .metricsReporter(resolvedMetricsReporter)
                .payloadProviderFactory(payloadProviderFactory)
                .schedulingConfig(ProfileFactories.schedulingConfig(properties))
                .build();
        engine.start();
        return engine;
    }
}
