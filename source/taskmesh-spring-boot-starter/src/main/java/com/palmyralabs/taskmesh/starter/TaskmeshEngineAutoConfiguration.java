package com.palmyralabs.taskmesh.starter;

import com.palmyralabs.taskmesh.api.AuditEventSink;
import com.palmyralabs.taskmesh.api.ClaimStrategy;
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
import com.palmyralabs.taskmesh.engine.impl.DefaultNodeInfoProvider;
import com.palmyralabs.taskmesh.engine.impl.SchedulingConfig;
import com.palmyralabs.taskmesh.impl.ExponentialBackoffRetryPolicy;
import com.palmyralabs.taskmesh.impl.JpaAuditEventSink;
import com.palmyralabs.taskmesh.impl.JpaIncidentStore;
import com.palmyralabs.taskmesh.impl.JpaPauseController;
import com.palmyralabs.taskmesh.impl.repository.TmAttemptHistoryRepository;
import com.palmyralabs.taskmesh.impl.repository.TmAuditEventRepository;
import com.palmyralabs.taskmesh.impl.repository.TmControlFlagRepository;
import com.palmyralabs.taskmesh.impl.repository.TmHistoryEventRepository;
import com.palmyralabs.taskmesh.impl.repository.TmInFlightRepository;
import com.palmyralabs.taskmesh.impl.repository.TmIncidentRepository;
import com.palmyralabs.taskmesh.impl.repository.TmTaskRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;

/**
 * Auto-configures a {@link TaskMeshEngine} bean and its collaborators, given
 * that the {@code Tm*Repository} beans already exist somewhere (either from
 * {@link TaskmeshJpaAutoConfiguration} or a consumer's own JPA setup — see
 * its Javadoc). Every collaborator bean is individually {@code @ConditionalOnMissingBean}:
 * a consuming app can override exactly one piece (e.g. supply its own
 * {@link HistoryWriter}) and every other piece still gets its profile default
 * (D28/D29 — {@link WorkloadProfile}, {@link ProfileFactories}).
 *
 * <p>Requires the consuming app to define its own {@code List<TaskProcessor>}
 * bean — deliberately not auto-discovered (explicit per-engine processor
 * list, not "every TaskProcessor in the context").
 */
@AutoConfiguration
@EnableConfigurationProperties(TaskmeshProperties.class)
@ConditionalOnBean(TmTaskRepository.class)
public class TaskmeshEngineAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NodeInfoProvider nodeInfoProvider(TaskmeshProperties properties) {
        return properties.getNodeId().isBlank() ? new DefaultNodeInfoProvider() : properties::getNodeId;
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy retryPolicy(TaskmeshProperties properties) {
        return new ExponentialBackoffRetryPolicy(properties.getMaxAttempts(),
                Duration.ofMinutes(properties.getBackoffBaseMinutes()), Duration.ofMinutes(properties.getBackoffCapMinutes()));
    }

    @Bean
    @ConditionalOnMissingBean
    public MetricsReporter metricsReporter() {
        return new Slf4jMetricsReporter();
    }

    @Bean
    @ConditionalOnMissingBean
    public PauseController pauseController(TmControlFlagRepository repository, TaskmeshProperties properties) {
        return new JpaPauseController(repository, Duration.ofSeconds(properties.getQueryIntervalSeconds()));
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
        return ProfileFactories.claimStrategy(properties.getProfile(), properties, taskRepository, inFlightRepository,
                transactionTemplate, nodeInfoProvider, retryPolicy);
    }

    /**
     * Same instance as {@link #claimStrategy} — {@code SimpleTaskLedger}/{@code MultiNodeTaskLedger}
     * implement both, so Spring's by-type autowiring can't disambiguate the
     * two beans on its own (each one's runtime type satisfies both
     * interfaces) — every injection point below needs an explicit
     * {@code @Qualifier} naming which bean, not just the interface type.
     */
    @Bean
    @ConditionalOnMissingBean(TaskLedger.class)
    public TaskLedger taskLedger(@Qualifier("claimStrategy") ClaimStrategy claimStrategy) {
        return (TaskLedger) claimStrategy;
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

    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    public TaskMeshEngine taskMeshEngine(TaskmeshProperties properties, NodeInfoProvider nodeInfoProvider,
            List<TaskProcessor> processors, @Qualifier("claimStrategy") ClaimStrategy claimStrategy,
            @Qualifier("taskLedger") TaskLedger taskLedger,
            HeartbeatStrategy heartbeatStrategy, PauseController pauseController, HistoryWriter historyWriter,
            AuditEventSink auditEventSink, IncidentStore incidentStore, MetricsReporter metricsReporter,
            PayloadProviderFactory payloadProviderFactory) {
        TaskMeshEngine engine = TaskMeshEngine.builder()
                .nodeInfoProvider(nodeInfoProvider)
                .processors(processors)
                .claimStrategy(claimStrategy)
                .taskLedger(taskLedger)
                .heartbeatStrategy(heartbeatStrategy)
                .pauseController(pauseController)
                .historyWriter(historyWriter)
                .auditEventSink(auditEventSink)
                .incidentStore(incidentStore)
                .metricsReporter(metricsReporter)
                .payloadProviderFactory(payloadProviderFactory)
                .schedulingConfig(SchedulingConfig.builder()
                        .workerPoolSize(properties.getWorkerPoolSize())
                        .batchSize(properties.getBatchSize())
                        .queryIntervalSeconds(properties.getQueryIntervalSeconds())
                        .build())
                .build();
        engine.start();
        return engine;
    }
}
