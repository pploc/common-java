package com.gym.common.kafka.config;

import com.google.protobuf.Message;
import com.gym.common.kafka.consumer.ConfluentProtobufRecordDecoder;
import com.gym.common.kafka.consumer.DeliverySleeper;
import com.gym.common.kafka.consumer.RawDeliveryCoordinatorFactory;
import com.gym.common.kafka.consumer.RawDlqPublisher;
import com.gym.common.kafka.consumer.RawKafkaDecoder;
import com.gym.common.kafka.consumer.RawKafkaDlqPublisher;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@EnableKafka
@ConditionalOnClass(KafkaTemplate.class)
@EnableConfigurationProperties(KafkaEventProperties.class)
public class KafkaAutoConfig {
    private final KafkaEventProperties kafkaEventProperties;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers = "localhost:9092";

    @Value("${spring.kafka.consumer.group-id:ms-gym-member-v2}")
    private String defaultGroupId = "ms-gym-member-v2";

    public KafkaAutoConfig(KafkaEventProperties kafkaEventProperties) {
        this.kafkaEventProperties = kafkaEventProperties;
    }

    @Bean
    public ProducerFactory<String, Message> producerFactory() {
        Map<String, Object> config = producerConfig();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        config.put("schema.registry.url", kafkaEventProperties.getSchemaRegistryUrl());
        config.put("value.subject.name.strategy", kafkaEventProperties.getValueSubjectNameStrategy());
        config.put("auto.register.schemas", kafkaEventProperties.isAutoRegisterSchemas());
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Message> kafkaTemplate(
            @Qualifier("producerFactory") ProducerFactory<String, Message> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }

    /** Raw values are restricted to the DLQ path so malformed frames are never reserialized. */
    @Bean
    public ProducerFactory<byte[], byte[]> rawKafkaProducerFactory() {
        Map<String, Object> config = producerConfig();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<byte[], byte[]> rawKafkaTemplate(
            @Qualifier("rawKafkaProducerFactory") ProducerFactory<byte[], byte[]> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }

    /** Raw ingress preserves the complete Confluent frame and every broker header before decoding. */
    @Bean
    public ConsumerFactory<byte[], byte[]> rawKafkaConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<byte[], byte[]> rawKafkaListenerContainerFactory(
            @Qualifier("rawKafkaConsumerFactory") ConsumerFactory<byte[], byte[]> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<byte[], byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean(destroyMethod = "close")
    public RawKafkaDecoder rawKafkaDecoder() {
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", kafkaEventProperties.getSchemaRegistryUrl());
        config.put("value.subject.name.strategy", kafkaEventProperties.getValueSubjectNameStrategy());
        config.put("auto.register.schemas", false);
        KafkaProtobufDeserializer<Message> deserializer = new KafkaProtobufDeserializer<>();
        deserializer.configure(config, false);
        return new ConfluentProtobufRecordDecoder(deserializer);
    }

    @Bean
    public RawDlqPublisher rawDlqPublisher(
            @Qualifier("rawKafkaTemplate") KafkaTemplate<byte[], byte[]> kafkaTemplate
    ) {
        return new RawKafkaDlqPublisher(
                kafkaTemplate,
                kafkaEventProperties.getDlq().getSuffix(),
                kafkaEventProperties.getPublishTimeout().toMillis(),
                Clock.systemUTC()
        );
    }

    @Bean
    public DeliverySleeper deliverySleeper() {
        return duration -> Thread.sleep(duration.toMillis());
    }

    @Bean
    public RawDeliveryCoordinatorFactory rawDeliveryCoordinatorFactory(
            RawKafkaDecoder decoder,
            RawDlqPublisher dlqPublisher,
            DeliverySleeper sleeper
    ) {
        return new RawDeliveryCoordinatorFactory(decoder, dlqPublisher, sleeper);
    }

    private Map<String, Object> producerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return config;
    }
}
