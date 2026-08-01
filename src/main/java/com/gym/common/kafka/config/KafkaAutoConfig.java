package com.gym.common.kafka.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@EnableKafka
@ConditionalOnClass(KafkaTemplate.class)
@EnableConfigurationProperties(KafkaEventProperties.class)
@RequiredArgsConstructor
public class KafkaAutoConfig {

    public static final String HEADER_ORIGINAL_TOPIC = "x-original-topic";
    public static final String HEADER_EXCEPTION_MESSAGE = "x-exception-message";
    public static final String HEADER_FAILED_AT = "x-failed-at";
    public static final String HEADER_RETRY_COUNT = "x-retry-count";

    private final KafkaEventProperties kafkaEventProperties;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:ms-gym-member-group}")
    private String defaultGroupId;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, kafkaEventProperties.getTrustedPackages());
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + kafkaEventProperties.getDlq().getSuffix(), record.partition()));

        recoverer.setHeadersFunction(KafkaAutoConfig::createDlqHeaders);

        // Exponential backoff configured from properties
        ExponentialBackOff backOff = new ExponentialBackOff(
                kafkaEventProperties.getBackoff().getInitialInterval().toMillis(),
                kafkaEventProperties.getBackoff().getMultiplier()
        );
        backOff.setMaxInterval(kafkaEventProperties.getBackoff().getMaxInterval().toMillis());
        backOff.setMaxElapsedTime(kafkaEventProperties.getBackoff().getMaxElapsedTime().toMillis());

        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public RetryTopicConfiguration retryTopicConfiguration(KafkaTemplate<String, Object> kafkaTemplate) {
        if (!kafkaEventProperties.getRetry().isEnabled()) {
            return null;
        }
        return RetryTopicConfigurationBuilder.newInstance()
                .maxAttempts(kafkaEventProperties.getRetry().getMaxAttempts())
                .exponentialBackoff(
                        kafkaEventProperties.getRetry().getInitialInterval().toMillis(),
                        kafkaEventProperties.getRetry().getMultiplier(),
                        kafkaEventProperties.getRetry().getMaxInterval().toMillis()
                )
                .useSingleTopicForSameIntervals()
                .create(kafkaTemplate);
    }

    public static Headers createDlqHeaders(ConsumerRecord<?, ?> record, Exception exception) {
        Headers headers = new RecordHeaders();
        headers.add(HEADER_ORIGINAL_TOPIC, record.topic().getBytes(StandardCharsets.UTF_8));

        String excMsg = exception.getCause() != null && exception.getCause().getMessage() != null
                ? exception.getCause().getMessage()
                : (exception.getMessage() != null ? exception.getMessage() : "Unknown error");
        headers.add(HEADER_EXCEPTION_MESSAGE, excMsg.getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_FAILED_AT, String.valueOf(Instant.now().toEpochMilli()).getBytes(StandardCharsets.UTF_8));

        int attempt = 1;
        org.apache.kafka.common.header.Header countHeader = record.headers().lastHeader(HEADER_RETRY_COUNT);
        if (countHeader != null && countHeader.value() != null) {
            attempt = Integer.parseInt(new String(countHeader.value(), StandardCharsets.UTF_8)) + 1;
        }
        headers.add(HEADER_RETRY_COUNT, String.valueOf(attempt).getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
