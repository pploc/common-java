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
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

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
    private String bootstrapServers = "localhost:9092";

    @Value("${spring.kafka.consumer.group-id:ms-gym-member-group}")
    private String defaultGroupId = "ms-gym-member-group";

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        config.put("schema.registry.url", kafkaEventProperties.getSchemaRegistryUrl());
        config.put("value.subject.name.strategy", kafkaEventProperties.getValueSubjectNameStrategy());
        config.put("auto.register.schemas", kafkaEventProperties.isAutoRegisterSchemas());
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
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaProtobufDeserializer.class);
        config.put("schema.registry.url", kafkaEventProperties.getSchemaRegistryUrl());
        config.put("value.subject.name.strategy", kafkaEventProperties.getValueSubjectNameStrategy());
        config.put("auto.register.schemas", kafkaEventProperties.isAutoRegisterSchemas());
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + kafkaEventProperties.getDlq().getSuffix(), record.partition()));
        recoverer.setHeadersFunction(KafkaAutoConfig::createDlqHeaders);

        var retry = kafkaEventProperties.getRetry();
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(
                retry.isEnabled() ? retry.getRetryCount() : 0
        );
        backOff.setInitialInterval(retry.getInitialInterval().toMillis());
        backOff.setMultiplier(retry.getMultiplier());
        backOff.setMaxInterval(retry.getMaxInterval().toMillis());

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    public static Headers createDlqHeaders(ConsumerRecord<?, ?> record, Exception exception) {
        Headers headers = new RecordHeaders();
        headers.add(HEADER_ORIGINAL_TOPIC, record.topic().getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_EXCEPTION_MESSAGE, clientSafeDiagnostic(exception).getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_FAILED_AT, String.valueOf(Instant.now().toEpochMilli()).getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_RETRY_COUNT, String.valueOf(nextRetryCount(record)).getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    private static String clientSafeDiagnostic(Exception exception) {
        return exception == null ? "Kafka message processing failed" : exception.getClass().getSimpleName();
    }

    private static int nextRetryCount(ConsumerRecord<?, ?> record) {
        org.apache.kafka.common.header.Header countHeader = record.headers().lastHeader(HEADER_RETRY_COUNT);
        if (countHeader == null || countHeader.value() == null) {
            return 1;
        }

        try {
            int retryCount = Integer.parseInt(new String(countHeader.value(), StandardCharsets.UTF_8));
            return retryCount >= 0 ? retryCount + 1 : 1;
        } catch (NumberFormatException ignored) {
            return 1;
        }
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
