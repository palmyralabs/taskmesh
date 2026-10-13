package com.palmyralabs.taskmesh.max.starter;

import com.palmyralabs.taskmesh.max.SpringTransactionRunner;
import com.palmyralabs.taskmesh.api.AuditEventSink;
import com.palmyralabs.taskmesh.api.BacklogPromoter;
import com.palmyralabs.taskmesh.api.ClaimStrategy;
import com.palmyralabs.taskmesh.api.FailureClassifier;
import com.palmyralabs.taskmesh.api.HeartbeatStrategy;
import com.palmyralabs.taskmesh.api.HistoryWriter;
import com.palmyralabs.taskmesh.api.IncidentStore;
import com.palmyralabs.taskmesh.api.MetricsReporter;
import com.palmyralabs.taskmesh.api.NodeInfoProvider;
import com.palmyralabs.taskmesh.api.PauseController;
import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import com.palmyralabs.taskmesh.api.RetryPolicy;
import com.palmyralabs.taskmesh.api.TaskLedger;
import com.palmyralabs.taskmesh.api.TaskProcessor;
import com.palmyralabs.taskmesh.engine.TaskMeshEngine;
import com.palmyralabs.taskmesh.engine.impl.DefaultFailureClassifier;
import com.palmyralabs.taskmesh.engine.impl.DefaultNodeInfoProvider;
import com.palmyralabs.taskmesh.engine.impl.SchedulingConfig;
import com.palmyralabs.taskmesh.max.ExponentialBackoffRetryPolicy;
import com.palmyralabs.taskmesh.max.JpaAuditEventSink;
import com.palmyralabs.taskmesh.max.JpaIncidentStore;
import com.palmyralabs.taskmesh.max.JpaPauseController;
import com.palmyralabs.taskmesh.max.NodeHeartbeatWriter;
import com.palmyralabs.taskmesh.max.NoopBacklogPromoter;
import com.palmyralabs.taskmesh.max.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.max.repository.TmAuditEventRepository;
import com.palmyralabs.taskmesh.max.repository.TmControlFlagRepository;
import com.palmyralabs.taskmesh.max.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.max.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.max.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.max.repository.TmNodeRepository;
import com.palmyralabs.taskmesh.max.repository.TmTaskRepository;
import com.palmyralabs.taskmesh.starter.SingleProviderPayloadProviderFactory;
import com.palmyralabs.taskmesh.starter.Slf4jMetricsReporter;
import com.palmyralabs.taskmesh.starter.TaskmeshProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;

/**
 * Auto-configures a {@link TaskMeshEngine} bean and its collaborators for the
 * taskmesh-max tier, given that the {@code Tm*Repository} beans already
 * exist somewhere (either from {@link TaskmeshJpaAutoConfiguration} or a
 * consumer's own JPA setup). Every collaborator bean is individually
 * {@code @ConditionalOnMissingBean}: a consuming app can override exactly one
 * piece and every other piece still gets its profile default ({@link
 * ProfileFactories}).
 *
 * <p>Requires the consuming app to define its own {@code List<TaskProcessor>}
 * bean — deliberately not auto-discovered.
 */
@AutoConfiguration(after = TaskmeshJpaAutoConfiguration.class)
@EnableConfigurationProperties(TaskmeshProperties.class)
@ConditionalOnBean(TmTaskRepository.class)
@EnableScheduling
@Slf4j
public class TaskmeshEngineAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NodeInfoProvider nodeInfoProvider(TaskmeshProperties properties) {
        return properties.getNodeId().isBlank() ? new DefaultNodeInfoProvider() : properties::getNodeId;
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy retryPolicy(TaskmeshProperties properties) {
        return new ExponentialBackoffRetryPolicy(properties.getMaxRetries(),
                Duration.ofMinutes(properties.getBackoffBaseMinutes()), Duration.ofMinutes(properties.getBackoffCapMinutes()),
                properties.getBackoffJitter());
    }

    @Bean
    @ConditionalOnMissingBean
    public MetricsReporter metricsReporter() {
        return new Slf4jMetricsReporter();
    }

    @Bean
    @ConditionalOnMissingBean
    public PauseController pauseController(TmControlFlagRepository controlFlagRepository,
            org.springframework.beans.factory.ObjectProvider<TmNodeRepository> nodeRepositoryProvider,
            NodeInfoProvider nodeInfoProvider, TaskmeshProperties properties) {
        return new JpaPauseController(controlFlagRepository, nodeRepositoryProvider.getIfAvailable(),
                nodeInfoProvider, Duration.ofSeconds(properties.getQueryIntervalSeconds()));
    }

    @Bean
    @ConditionalOnMissingBean
    public AuditEventSink auditEventSink(TmAuditEventRepository repository) {
        return new JpaAuditEventSink(repository);
    }

    @Bean
    @ConditionalOnMissingBean
    public IncidentStore incidentStore(TmIncidentRepository repository) {
        return new JpaIncidentStore(repository);
    }

    /** Only activates when exactly one {@link PayloadProvider} bean exists — multi-task_type apps must supply their own factory. */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnSingleCandidate(PayloadProvider.class)
    public PayloadProviderFactory payloadProviderFactory(PayloadProvider provider) {
        return new SingleProviderPayloadProviderFactory(provider);
    }

    @Bean
    @ConditionalOnMissingBean(name = "taskmeshTransactionTemplate")
    public TransactionTemplate taskmeshTransactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }

    @Bean
    @ConditionalOnMissingBean(ClaimStrategy.class)
    public ClaimStrategy claimStrategy(TaskmeshProperties properties, TmTaskRepository taskRepository,
            TmInFlightRepository inFlightRepository, @Qualifier("taskmeshTransactionTemplate") TransactionTemplate transactionTemplate,
            NodeInfoProvider nodeInfoProvider, RetryPolicy retryPolicy) {
        return ProfileFactories.claimStrategy(properties, taskRepository, inFlightRepository,
                transactionTemplate, nodeInfoProvider, retryPolicy);
    }

    /**
     * Same instance as {@link #claimStrategy} — {@code PartitionedTaskLedger}
     * implements both {@code ClaimStrategy} and {@code TaskLedger}, so
     * Spring's by-type autowiring can't disambiguate the two beans on its
     * own — every injection point below needs an explicit {@code @Qualifier}
     * naming which bean, not just the interface type.
     */
    @Bean
    @ConditionalOnMissingBean(TaskLedger.class)
    public TaskLedger taskLedger(@Qualifier("claimStrategy") ClaimStrategy claimStrategy) {
        return (TaskLedger) claimStrategy;
    }

    /**
     * Selected by {@code taskmesh.backlog-promotion}, defaulting to {@code EAGER}
     * ({@link NoopBacklogPromoter}) — this tier's {@code enqueue()} only writes
     * {@code tm_task}, so Noop's {@code onEnqueue} creates the hot row immediately
     * here (reproducing the eager-pair behavior), unlike taskmesh-simple/-pro where
     * {@code enqueue()} itself already does that. {@code THROTTLED} swaps in
     * {@code JpaBacklogPromoter} and its in-flight window. A consumer wanting
     * something else entirely still just supplies their own bean.
     */
    @Bean
    @ConditionalOnMissingBean(BacklogPromoter.class)
    public BacklogPromoter backlogPromoter(TaskmeshProperties properties, TmTaskRepository taskRepository,
            TmInFlightRepository inFlightRepository, @Qualifier("claimStrategy") ClaimStrategy claimStrategy,
            NodeInfoProvider nodeInfoProvider,
            @Qualifier("taskmeshTransactionTemplate") TransactionTemplate transactionTemplate) {
        return ProfileFactories.backlogPromoter(properties, taskRepository, inFlightRepository,
                transactionTemplate, claimStrategy, nodeInfoProvider);
    }

    @Bean
    @ConditionalOnMissingBean(HeartbeatStrategy.class)
    public HeartbeatStrategy heartbeatStrategy(TaskmeshProperties properties,
            @Qualifier("claimStrategy") ClaimStrategy claimStrategy, IncidentStore incidentStore) {
        return ProfileFactories.heartbeatStrategy(properties.getProfile(), properties, claimStrategy, incidentStore);
    }

    @Bean
    @ConditionalOnMissingBean(HistoryWriter.class)
    public HistoryWriter historyWriter(TaskmeshProperties properties, TmHistoryEventRepository historyEventRepository,
            TmAttemptHistoryRepository attemptHistoryRepository) {
        return ProfileFactories.historyWriter(properties.getProfile(), historyEventRepository, attemptHistoryRepository);
    }

    /**
     * Node-presence roster, independent of the engine itself — see {@link
     * NodeHeartbeatWriter}'s javadoc for why this is not {@link HeartbeatStrategy}.
     * Requires {@link TmNodeRepository} to already exist in the context (same
     * package-scan mechanism that surfaces every other {@code Tm*Repository}
     * here), so a consumer who has not added {@code tm_node} to their schema
     * yet simply does not get this bean rather than failing to start.
     *
     * <p>Also gated on {@code taskmesh.enabled} (default {@code true}, same
     * flag {@link #taskMeshEngine} reads): a disabled node never claims or
     * executes any task, so it has no business appearing in the cluster's
     * node-presence roster either — {@code tm_node} answers "which nodes are
     * actually part of the running cluster", not "which JVMs happen to have
     * this dependency on their classpath". Added 2026-08-08 after a
     * dashboard-facing node showed up in {@code tm_node} despite never running
     * the engine.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(TmNodeRepository.class)
    @ConditionalOnProperty(prefix = "taskmesh", name = "enabled", havingValue = "true", matchIfMissing = true)
    public NodeHeartbeatWriter nodeHeartbeatWriter(TmNodeRepository repository, NodeInfoProvider nodeInfoProvider) {
        return new NodeHeartbeatWriter(repository, nodeInfoProvider);
    }

    /**
     * The engine's failure taxonomy. Declared as a bean rather than constructed
     * inline so an application can replace it the same way it replaces any other
     * collaborator here -- by defining its own, which this then backs off from.
     *
     * <p>Not academic: an application whose classifier is never consulted has
     * every permanent failure retried to the end of the budget, and its reason
     * codes never reach {@code TerminalContext.reasonCode()}.
     */
    @Bean
    @ConditionalOnMissingBean
    public FailureClassifier failureClassifier() {
        return new DefaultFailureClassifier();
    }

    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    public TaskMeshEngine taskMeshEngine(TaskmeshProperties properties, NodeInfoProvider nodeInfoProvider,
            List<TaskProcessor> processors, @Qualifier("claimStrategy") ClaimStrategy claimStrategy,
            @Qualifier("taskLedger") TaskLedger taskLedger, BacklogPromoter backlogPromoter,
            HeartbeatStrategy heartbeatStrategy, PauseController pauseController, HistoryWriter historyWriter,
            AuditEventSink auditEventSink, IncidentStore incidentStore, MetricsReporter metricsReporter,
            PayloadProviderFactory payloadProviderFactory, FailureClassifier failureClassifier,
            @Qualifier("taskmeshTransactionTemplate") TransactionTemplate transactionTemplate) {
        TaskMeshEngine engine = TaskMeshEngine.builder()
                .transactionRunner(new SpringTransactionRunner(transactionTemplate))
                .nodeInfoProvider(nodeInfoProvider)
                .processors(processors)
                .claimStrategy(claimStrategy)
                .taskLedger(taskLedger)
                .backlogPromoter(backlogPromoter)
                .heartbeatStrategy(heartbeatStrategy)
                .pauseController(pauseController)
                .historyWriter(historyWriter)
                .auditEventSink(auditEventSink)
                .incidentStore(incidentStore)
                .metricsReporter(metricsReporter)
                .failureClassifier(failureClassifier)
                .payloadProviderFactory(payloadProviderFactory)
                .schedulingConfig(ProfileFactories.schedulingConfig(properties))
                .build();
        if (properties.isEnabled()) {
            engine.start();
        } else {
            log.info("taskmesh.enabled=false — engine wired but not started on this node; "
                    + "it will not poll, claim or execute any task");
        }
        return engine;
    }
}
