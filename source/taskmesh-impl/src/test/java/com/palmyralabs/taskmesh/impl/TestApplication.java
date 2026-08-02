package com.palmyralabs.taskmesh.impl;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

/**
 * This module is a library, not an app — no {@code @SpringBootApplication}
 * exists to anchor {@code @DataJpaTest}'s component scan. This minimal
 * config is that anchor, test-only.
 */
@SpringBootConfiguration
@EnableAutoConfiguration
public class TestApplication {
}
