package com.gym.common.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "gym.kafka")
public class KafkaEventProperties {
    private String schemaRegistryUrl = "http://localhost:8081";
    private String valueSubjectNameStrategy = "io.confluent.kafka.serializers.subject.TopicNameStrategy";
    private boolean autoRegisterSchemas = false;
    private final Dlq dlq = new Dlq();
    private final Retry retry = new Retry();
    private Duration publishTimeout = Duration.ofMillis(25000);

    @Getter
    @Setter
    public static class Dlq {
        private String suffix = ".DLQ";
    }

    @Getter
    @Setter
    public static class Retry {
        private boolean enabled = true;
        private int retryCount = 3;
        private Duration initialInterval = Duration.ofSeconds(2);
        private double multiplier = 2.0;
        private Duration maxInterval = Duration.ofSeconds(8);
    }
}
