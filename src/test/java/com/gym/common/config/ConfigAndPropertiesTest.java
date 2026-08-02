package com.gym.common.config;

import com.gym.common.grpc.config.GrpcProperties;
import com.gym.common.grpc.config.GrpcServerAutoConfig;
import com.gym.common.grpc.interceptor.GrpcMethodRegistry;
import com.gym.common.kafka.config.KafkaEventProperties;
import com.gym.common.kafka.config.KafkaAutoConfig;
import org.junit.jupiter.api.Test;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfigAndPropertiesTest {

    @Test
    void testGrpcProperties() {
        GrpcProperties properties = new GrpcProperties();
        assertEquals(9090, properties.getPort());
        assertEquals(Duration.ofSeconds(15), properties.getShutdownTimeout());

        properties.setPort(9091);
        properties.setShutdownTimeout(Duration.ofSeconds(30));
        assertEquals(9091, properties.getPort());
        assertEquals(Duration.ofSeconds(30), properties.getShutdownTimeout());
    }

    @Test
    void testKafkaEventProperties() {
        KafkaEventProperties props = new KafkaEventProperties();
        assertEquals("com.gym.*", props.getTrustedPackages());
        assertEquals("http://localhost:8081", props.getSchemaRegistryUrl());
        assertEquals("io.confluent.kafka.serializers.subject.TopicNameStrategy", props.getValueSubjectNameStrategy());
        assertFalse(props.isAutoRegisterSchemas());
        assertEquals(".DLQ", props.getDlq().getSuffix());
        assertEquals(3, props.getRetry().getRetryCount());
        assertEquals(Duration.ofSeconds(2), props.getRetry().getInitialInterval());
        assertEquals(2.0, props.getRetry().getMultiplier());
        assertEquals(Duration.ofSeconds(8), props.getRetry().getMaxInterval());

        props.setTrustedPackages("com.custom.*");
        props.setAutoRegisterSchemas(true);
        assertEquals("com.custom.*", props.getTrustedPackages());
        assertTrue(props.isAutoRegisterSchemas());
    }

    @Test
    void testWebCommonConfigBeans() {
        WebCommonConfig webConfig = new WebCommonConfig();
        assertNotNull(webConfig.errorResponseFactory());
        assertNotNull(webConfig.globalExceptionHandler(webConfig.errorResponseFactory()));
        assertNotNull(webConfig.correlationIdFilter());
    }

    @Test
    void testGrpcServerAutoConfigBeans() {
        GrpcProperties properties = new GrpcProperties();
        GrpcServerAutoConfig config = new GrpcServerAutoConfig(properties);
        GrpcMethodRegistry registry = mock(GrpcMethodRegistry.class);

        assertNotNull(config.authServerInterceptor(registry));
        assertNotNull(config.exceptionInterceptor());
        assertNotNull(config.loggingInterceptor());
        assertNotNull(config.tracingInterceptor());
    }

    @Test
    void testKafkaAutoConfigBeans() {
        KafkaEventProperties props = new KafkaEventProperties();
        KafkaAutoConfig config = new KafkaAutoConfig(props);
        org.springframework.test.util.ReflectionTestUtils.setField(config, "bootstrapServers", "localhost:9092");

        DefaultKafkaProducerFactory<String, Object> producerFactory =
                (DefaultKafkaProducerFactory<String, Object>) config.producerFactory();
        Map<String, Object> producerConfig = producerFactory.getConfigurationProperties();
        assertEquals(KafkaProtobufSerializer.class, producerConfig.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("all", producerConfig.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, producerConfig.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(false, producerConfig.get("auto.register.schemas"));
        assertEquals("io.confluent.kafka.serializers.subject.TopicNameStrategy",
                producerConfig.get("value.subject.name.strategy"));

        KafkaTemplate<String, Object> template = config.kafkaTemplate(producerFactory);
        assertNotNull(template);

        DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                (DefaultKafkaConsumerFactory<String, Object>) config.consumerFactory();
        Map<String, Object> consumerConfig = consumerFactory.getConfigurationProperties();
        assertEquals(KafkaProtobufDeserializer.class,
                consumerConfig.get("spring.deserializer.value.delegate.class"));
        assertEquals(false, consumerConfig.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(false, consumerConfig.get("auto.register.schemas"));

        assertNotNull(config.errorHandler(template));
        assertNotNull(config.kafkaListenerContainerFactory(consumerFactory, config.errorHandler(template)));
    }
}
