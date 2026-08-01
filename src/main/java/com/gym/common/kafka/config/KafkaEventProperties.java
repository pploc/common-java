package com.gym.common.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "gym.kafka")
public class KafkaEventProperties {
    private String trustedPackages = "com.gym.*";
    private final Dlq dlq = new Dlq();
    private final Backoff backoff = new Backoff();
    private final Retry retry = new Retry();
    private Duration publishTimeout = Duration.ofMillis(25000);

    @Getter
    @Setter
    public static class Dlq {
        private String suffix = ".DLQ";
    }

    @Getter
    @Setter
    public static class Backoff {
        private Duration initialInterval = Duration.ofSeconds(2);
        private double multiplier = 2.0;
        private Duration maxInterval = Duration.ofSeconds(8);
        private Duration maxElapsedTime = Duration.ofSeconds(15);
    }

    @Getter
    @Setter
    public static class Retry {
        private boolean enabled = true;
        private int maxAttempts = 3;
        private Duration initialInterval = Duration.ofSeconds(1);
        private double multiplier = 2.0;
        private Duration maxInterval = Duration.ofSeconds(8);
    }
}
