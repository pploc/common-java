package com.gym.common.config;

import com.gym.common.grpc.config.GrpcProperties;
import com.gym.common.grpc.config.GrpcServerAutoConfig;
import com.gym.common.grpc.interceptor.GrpcMethodRegistry;
import com.gym.common.kafka.config.KafkaEventProperties;
import com.gym.common.kafka.config.KafkaAutoConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;

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
        assertEquals(".DLQ", props.getDlq().getSuffix());
        assertEquals(Duration.ofSeconds(2), props.getBackoff().getInitialInterval());
        assertEquals(2.0, props.getBackoff().getMultiplier());

        props.setTrustedPackages("com.custom.*");
        assertEquals("com.custom.*", props.getTrustedPackages());
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

        assertNotNull(config.producerFactory());
        KafkaTemplate<String, Object> template = config.kafkaTemplate(config.producerFactory());
        assertNotNull(template);
        assertNotNull(config.consumerFactory());
        assertNotNull(config.errorHandler(template));
        assertNotNull(config.kafkaListenerContainerFactory(config.consumerFactory(), config.errorHandler(template)));
    }
}
