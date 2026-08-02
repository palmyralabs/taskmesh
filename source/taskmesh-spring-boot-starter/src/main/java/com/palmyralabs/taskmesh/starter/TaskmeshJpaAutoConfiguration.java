package com.palmyralabs.taskmesh.starter;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Zero-config JPA wiring for taskmesh's own tables — a dedicated persistence
 * unit ({@code taskmeshEntityManagerFactory}/{@code taskmeshTransactionManager})
 * pointed at whatever {@link DataSource} bean already exists, scanning only
 * {@code com.palmyralabs.taskmesh.impl.entity}/{@code .repository}. Does not
 * touch or require Spring Boot's own {@code DataSourceAutoConfiguration}/
 * {@code HibernateJpaAutoConfiguration} — an app with its own multi-datasource
 * setup (see the migration toolkit's {@code MigDataSourceConfig}) should
 * exclude this class and instead point its own {@code @EnableJpaRepositories}/
 * entity scan at taskmesh's packages directly; {@link TaskmeshEngineAutoConfiguration}
 * only needs the {@code Tm*Repository} beans to exist, not this specific wiring.
 */
@AutoConfiguration
@EnableJpaRepositories(basePackages = "com.palmyralabs.taskmesh.impl.repository",
        entityManagerFactoryRef = "taskmeshEntityManagerFactory", transactionManagerRef = "taskmeshTransactionManager")
public class TaskmeshJpaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "taskmeshEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean taskmeshEntityManagerFactory(DataSource dataSource,
            @Value("${spring.jpa.hibernate.ddl-auto:validate}") String ddlAuto,
            @Value("${spring.jpa.show-sql:false}") boolean showSql) {
        LocalContainerEntityManagerFactoryBean emf = new LocalContainerEntityManagerFactoryBean();
        emf.setDataSource(dataSource);
        emf.setPackagesToScan("com.palmyralabs.taskmesh.impl.entity");
        emf.setPersistenceUnitName("taskmesh");
        emf.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        Properties jpaProps = new Properties();
        jpaProps.put("hibernate.hbm2ddl.auto", ddlAuto);
        jpaProps.put("hibernate.show_sql", String.valueOf(showSql));
        jpaProps.put("hibernate.physical_naming_strategy",
                "org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy");
        jpaProps.put("hibernate.implicit_naming_strategy",
                "org.springframework.boot.orm.jpa.hibernate.SpringImplicitNamingStrategy");
        emf.setJpaProperties(jpaProps);
        return emf;
    }

    @Bean
    @ConditionalOnMissingBean(name = "taskmeshTransactionManager")
    public PlatformTransactionManager taskmeshTransactionManager(
            @Qualifier("taskmeshEntityManagerFactory") EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }
}
