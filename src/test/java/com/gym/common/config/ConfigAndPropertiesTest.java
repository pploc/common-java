package com.gym.common.config;

import com.google.protobuf.Message;
import com.gym.common.grpc.config.GrpcProperties;
import com.gym.common.grpc.config.GrpcServerAutoConfig;
import com.gym.common.grpc.interceptor.GrpcMethodRegistry;
import com.gym.common.grpc.security.WorkloadIdentityVerifier;
import com.gym.common.kafka.config.KafkaAutoConfig;
import com.gym.common.kafka.config.KafkaEventProperties;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        assertEquals("http://localhost:8081", props.getSchemaRegistryUrl());
        assertEquals("io.confluent.kafka.serializers.subject.TopicNameStrategy", props.getValueSubjectNameStrategy());
        assertFalse(props.isAutoRegisterSchemas());
        assertEquals(".DLQ", props.getDlq().getSuffix());
        props.setAutoRegisterSchemas(true);
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
        @SuppressWarnings("unchecked")
        ObjectProvider<WorkloadIdentityVerifier> verifierProvider = mock(ObjectProvider.class);
        when(verifierProvider.getIfAvailable(any(Supplier.class))).thenReturn(call -> false);

        assertNotNull(config.authServerInterceptor(registry, verifierProvider));
        assertNotNull(config.exceptionInterceptor());
        assertNotNull(config.loggingInterceptor());
        assertNotNull(config.tracingInterceptor());
    }

    @Test
    void givenServletWeb_whenProtobufJsonWebConfig_thenRegistersConverterConfigurer() {
        // Given
        ProtobufJsonWebConfig config = new ProtobufJsonWebConfig();

        // When / Then
        assertNotNull(config.protobufJsonWebMvcConfigurer());
    }

    @Test
    void givenKafkaConfiguration_whenBuildingTransportBeans_thenSeparatesProtobufAndRawBytePaths() {
        KafkaEventProperties props = new KafkaEventProperties();
        KafkaAutoConfig config = new KafkaAutoConfig(props);
        org.springframework.test.util.ReflectionTestUtils.setField(config, "bootstrapServers", "localhost:9092");

        DefaultKafkaProducerFactory<String, Message> protobufFactory =
                (DefaultKafkaProducerFactory<String, Message>) config.producerFactory();
        Map<String, Object> protobufConfig = protobufFactory.getConfigurationProperties();
        assertEquals(KafkaProtobufSerializer.class, protobufConfig.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("all", protobufConfig.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, protobufConfig.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(false, protobufConfig.get("auto.register.schemas"));

        KafkaTemplate<String, Message> template = config.kafkaTemplate(protobufFactory);
        assertNotNull(template);

        DefaultKafkaProducerFactory<byte[], byte[]> rawProducerFactory =
                (DefaultKafkaProducerFactory<byte[], byte[]>) config.rawKafkaProducerFactory();
        assertEquals(ByteArraySerializer.class,
                rawProducerFactory.getConfigurationProperties().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

        DefaultKafkaConsumerFactory<byte[], byte[]> rawConsumerFactory =
                (DefaultKafkaConsumerFactory<byte[], byte[]>) config.rawKafkaConsumerFactory();
        Map<String, Object> rawConsumerConfig = rawConsumerFactory.getConfigurationProperties();
        assertEquals(ByteArrayDeserializer.class, rawConsumerConfig.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals(false, rawConsumerConfig.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("ms-gym-member-v2", rawConsumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertNotNull(config.rawKafkaListenerContainerFactory(rawConsumerFactory));
    }
}
