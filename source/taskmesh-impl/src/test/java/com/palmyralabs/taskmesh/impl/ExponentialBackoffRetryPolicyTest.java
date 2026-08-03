package com.palmyralabs.taskmesh.impl;

import com.palmyralabs.taskmesh.impl.ExponentialBackoffRetryPolicy.Jitter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ExponentialBackoffRetryPolicyTest {

    @Test
    void noJitterIsDeterministic() {
        var policy = new ExponentialBackoffRetryPolicy(5, Duration.ofMinutes(2), Duration.ofHours(1), Jitter.NONE);

        Instant before = Instant.now();
        Instant next = policy.nextRetryAt(3);
        Instant after = Instant.now();

        // attempt 3 -> factor 4 -> 8 minutes
        assertThat(next).isBetween(before.plus(Duration.ofMinutes(8)), after.plus(Duration.ofMinutes(8)));
    }

    @Test
    void fullJitterStaysWithinZeroToDelay() {
        var policy = new ExponentialBackoffRetryPolicy(5, Duration.ofMinutes(2), Duration.ofHours(1), Jitter.FULL);

        Instant now = Instant.now();
        for (int i = 0; i < 50; i++) {
            Instant next = policy.nextRetryAt(3); // undelayed factor -> 8 minutes upper bound
            assertThat(next).isAfterOrEqualTo(now.minusSeconds(1));
            assertThat(next).isBefore(now.plus(Duration.ofMinutes(8)).plusSeconds(1));
        }
    }

    @Test
    void equalJitterStaysWithinHalfToFullDelay() {
        var policy = new ExponentialBackoffRetryPolicy(5, Duration.ofMinutes(2), Duration.ofHours(1), Jitter.EQUAL);

        Instant now = Instant.now();
        for (int i = 0; i < 50; i++) {
            Instant next = policy.nextRetryAt(3); // 8 minutes -> [4, 8] minutes
            assertThat(next).isAfterOrEqualTo(now.plus(Duration.ofMinutes(4)).minusSeconds(1));
            assertThat(next).isBefore(now.plus(Duration.ofMinutes(8)).plusSeconds(1));
        }
    }

    @Test
    void capIsAppliedBeforeJitter() {
        var policy = new ExponentialBackoffRetryPolicy(10, Duration.ofMinutes(2), Duration.ofMinutes(5), Jitter.FULL);

        Instant now = Instant.now();
        Instant next = policy.nextRetryAt(10); // factor huge -> capped at 5 minutes
        assertThat(next).isBefore(now.plus(Duration.ofMinutes(5)).plusSeconds(1));
    }
}
