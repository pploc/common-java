init## Directory Structure

```
common-java/
├── build.gradle
├── settings.gradle
└── src/
    ├── main/
    │   ├── java/com/gym/common/
    │   │   ├── config/
    │   │   │   └── CommonAutoConfiguration.java
    │   │   ├── error/
    │   │   │   ├── ConflictException.java
    │   │   │   ├── DomainException.java
    │   │   │   ├── ErrorCode.java
    │   │   │   ├── ForbiddenException.java
    │   │   │   └── NotFoundException.java
    │   │   ├── grpc/
    │   │   │   ├── config/
    │   │   │   │   └── GrpcServerAutoConfig.java
    │   │   │   ├── interceptor/
    │   │   │   │   ├── AuthServerInterceptor.java
    │   │   │   │   ├── ExceptionInterceptor.java
    │   │   │   │   ├── GrpcMethodRegistry.java
    │   │   │   │   ├── LoggingInterceptor.java
    │   │   │   │   ├── MetricsInterceptor.java
    │   │   │   │   └── TracingInterceptor.java
    │   │   │   └── security/
    │   │   │       ├── GrpcSecurityContext.java
    │   │   │       ├── RequireRole.java
    │   │   │       └── UserClaims.java
    │   │   ├── kafka/
    │   │   │   ├── config/
    │   │   │   │   └── KafkaAutoConfig.java
    │   │   │   ├── consumer/
    │   │   │   │   ├── EventConsumer.java
    │   │   │   │   └── RetryableConsumer.java
    │   │   │   ├── message/
    │   │   │   │   ├── EventEnvelope.java
    │   │   │   │   └── EventEnvelopeSerializer.java
    │   │   │   └── producer/
    │   │   │       ├── EventPublisher.java
    │   │   │       └── EventPublisherImpl.java
    │   │   ├── pagination/
    │   │   │   ├── CursorPage.java
    │   │   │   ├── CursorUtils.java
    │   │   │   └── PageMapper.java
    │   │   └── persistence/
    │   │       ├── AuditListener.java
    │   │       └── BaseEntity.java
    │   └── resources/
    │       └── META-INF/spring/
    │           └── org.springframework.boot.autoconfigure.AutoConfiguration.imports
    └── test/
        └── java/com/gym/common/
            ├── kafka/
            │   └── EmbeddedKafkaTestConfig.java
            └── testutil/
                ├── GrpcTestHelper.java
                ├── TestContainersConfig.java
                └── TestDataBuilder.java
```

## Dependency Management and Setup

### settings.gradle
```groovy
rootProject.name = 'common-java'
```

### build.gradle
```groovy
plugins {
    id 'java-library'
    id 'org.springframework.boot' version '4.0.0-M1'
    id 'io.spring.dependency-management' version '1.1.6'
}

group = 'com.gym'
version = '1.0.0'

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(26)
    }
}

repositories {
    mavenCentral()
    maven { url 'https://maven.pkg.github.com/pploc/gym-proto' }
}

dependencies {
    api 'org.springframework.boot:spring-boot-starter'
    api 'org.springframework.boot:spring-boot-starter-aop'
    api 'org.springframework.kafka:spring-kafka'
    
    api 'io.grpc:grpc-netty-shaded:1.68.0'
    api 'io.grpc:grpc-protobuf:1.68.0'
    api 'io.grpc:grpc-stub:1.68.0'
    api 'io.grpc:grpc-services:1.68.0'
    api 'com.google.protobuf:protobuf-java-util:3.25.1'
    
    api 'com.gym.proto:gym-proto-java:1.0.0'
    
    api 'io.opentelemetry:io.opentelemetry-api:1.43.0'
    api 'io.micrometer:micrometer-core'
    api 'io.micrometer:micrometer-registry-prometheus'
    
    compileOnly 'jakarta.persistence:jakarta.persistence-api:3.2.0'
    api 'com.fasterxml.jackson.core:jackson-databind'
    
    compileOnly 'org.projectlombok:lombok:1.18.36'
    annotationProcessor 'org.projectlombok:lombok:1.18.36'
    
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.springframework.kafka:spring-kafka-test'
    testImplementation 'org.testcontainers:junit-jupiter:1.20.1'
    testImplementation 'org.testcontainers:postgresql:1.20.1'
    testImplementation 'org.testcontainers:kafka:1.20.1'
}
```

### Auto-Configuration Registration
Imports file register beans automatically in target microservices.

`src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
```text
com.gym.common.grpc.config.GrpcServerAutoConfig
com.gym.common.kafka.config.KafkaAutoConfig
com.gym.common.config.CommonAutoConfiguration
```

---

## Technical Component Code Templates

### 1. Role Guard Annotation + Interceptor

#### security/RequireRole.java
```java
package com.gym.common.grpc.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequireRole {
    String[] value();
}
```

#### security/UserClaims.java
```java
package com.gym.common.grpc.security;

public record UserClaims(String userId, String role, String gymId) {}
```

#### security/GrpcSecurityContext.java
```java
package com.gym.common.grpc.security;

import io.grpc.Context;

public class GrpcSecurityContext {
    public static final Context.Key<UserClaims> CLAIMS_KEY = Context.key("user-claims");

    public static UserClaims getCurrentClaims() {
        return CLAIMS_KEY.get();
    }
    
    public static String getUserId() {
        UserClaims claims = getCurrentClaims();
        return claims != null ? claims.userId() : null;
    }

    public static String getRole() {
        UserClaims claims = getCurrentClaims();
        return claims != null ? claims.role() : null;
    }

    public static String getGymId() {
        UserClaims claims = getCurrentClaims();
        return claims != null ? claims.gymId() : null;
    }
}
```

#### interceptor/GrpcMethodRegistry.java
Scans gRPC services at startup. Maps protobuf paths to Java method reflections. Fast O(1) checks.
```java
package com.gym.common.grpc.interceptor;

import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class GrpcMethodRegistry implements ApplicationListener<ContextRefreshedEvent> {

    private final Map<String, Method> cache = new ConcurrentHashMap<>();
    private final ApplicationContext applicationContext;

    public GrpcMethodRegistry(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Map<String, BindableService> services = applicationContext.getBeansOfType(BindableService.class);
        for (BindableService service : services.values()) {
            Class<?> implClass = service.getClass();
            if (implClass.getName().contains("$$")) {
                implClass = implClass.getSuperclass();
            }

            for (Method method : implClass.getMethods()) {
                for (MethodDescriptor<?, ?> descriptor : service.bindService().getServiceDescriptor().getMethods()) {
                    String fullMethodName = descriptor.getFullMethodName();
                    String rpcMethodName = fullMethodName.substring(fullMethodName.indexOf('/') + 1);
                    
                    if (method.getName().equalsIgnoreCase(rpcMethodName)) {
                        cache.put(fullMethodName, method);
                    }
                }
            }
        }
    }

    public Method getJavaMethod(String fullMethodName) {
        return cache.get(fullMethodName);
    }
}
```

#### interceptor/AuthServerInterceptor.java
Extracts header values. Enforces role security based on annotation configuration. Uses standard gRPC context.
```java
package com.gym.common.grpc.interceptor;

import com.gym.common.grpc.security.GrpcSecurityContext;
import com.gym.common.grpc.security.RequireRole;
import com.gym.common.grpc.security.UserClaims;
import io.grpc.*;
import org.springframework.core.annotation.AnnotationUtils;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class AuthServerInterceptor implements ServerInterceptor {

    private final GrpcMethodRegistry methodRegistry;

    public AuthServerInterceptor(GrpcMethodRegistry methodRegistry) {
        this.methodRegistry = methodRegistry;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String userId = headers.get(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER));
        String role = headers.get(Metadata.Key.of("x-user-role", Metadata.ASCII_STRING_MARSHALLER));
        String gymId = headers.get(Metadata.Key.of("x-gym-id", Metadata.ASCII_STRING_MARSHALLER));

        UserClaims claims = new UserClaims(userId, role, gymId);

        Method method = methodRegistry.getJavaMethod(call.getMethodDescriptor().getFullMethodName());
        RequireRole requireRole = null;
        if (method != null) {
            requireRole = AnnotationUtils.findAnnotation(method, RequireRole.class);
            if (requireRole == null) {
                requireRole = AnnotationUtils.findAnnotation(method.getDeclaringClass(), RequireRole.class);
            }
        }

        if (requireRole != null) {
            if (userId == null || role == null) {
                call.close(Status.UNAUTHENTICATED.withDescription("Missing credentials"), new Metadata());
                return new ServerCall.Listener<ReqT>() {};
            }

            List<String> allowedRoles = Arrays.asList(requireRole.value());
            if (!allowedRoles.contains(role)) {
                call.close(Status.PERMISSION_DENIED.withDescription("Insufficient permissions"), new Metadata());
                return new ServerCall.Listener<ReqT>() {};
            }
        }

        Context newContext = Context.current().withValue(GrpcSecurityContext.CLAIMS_KEY, claims);
        return Contexts.interceptCall(newContext, call, headers, next);
    }
}
```

---

### 2. GrpcServerAutoConfig

#### grpc/config/GrpcServerAutoConfig.java
Detects all Spring bean implementations of `BindableService`. Intercepts them and spawns server.
```java
package com.gym.common.grpc.config;

import com.gym.common.grpc.interceptor.AuthServerInterceptor;
import com.gym.common.grpc.interceptor.ExceptionInterceptor;
import com.gym.common.grpc.interceptor.GrpcMethodRegistry;
import io.grpc.*;
import io.grpc.protobuf.services.ProtoReflectionService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@AutoConfiguration
@ConditionalOnClass(Server.class)
@Import(GrpcMethodRegistry.class)
public class GrpcServerAutoConfig {
    private static final Logger log = LoggerFactory.getLogger(GrpcServerAutoConfig.class);

    private Server server;

    @Value("${grpc.server.port:9090}")
    private int port;

    @Bean
    public AuthServerInterceptor authServerInterceptor(GrpcMethodRegistry registry) {
        return new AuthServerInterceptor(registry);
    }

    @Bean
    public ExceptionInterceptor exceptionInterceptor() {
        return new ExceptionInterceptor();
    }

    @Bean
    public Server grpcServer(
            ApplicationContext applicationContext,
            AuthServerInterceptor authInterceptor,
            ExceptionInterceptor exceptionInterceptor) {

        Map<String, BindableService> serviceBeans = applicationContext.getBeansOfType(BindableService.class);
        List<ServerServiceDefinition> services = new ArrayList<>();

        for (BindableService service : serviceBeans.values()) {
            ServerServiceDefinition intercepted = ServerInterceptors.intercept(
                    service,
                    authInterceptor,
                    exceptionInterceptor
            );
            services.add(intercepted);
            log.info("Registered gRPC service: {}", service.getClass().getSimpleName());
        }

        ServerBuilder<?> builder = ServerBuilder.forPort(port)
                .addService(ProtoReflectionService.newInstance());

        services.forEach(builder::addService);
        this.server = builder.build();
        return this.server;
    }

    @PostConstruct
    public void start() throws IOException {
        if (server != null) {
            server.start();
            log.info("gRPC server started on port {}", port);
            Thread awaitThread = new Thread(() -> {
                try {
                    server.awaitTermination();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("gRPC server execution thread interrupted");
                }
            });
            awaitThread.setDaemon(true);
            awaitThread.start();
        }
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (server != null) {
            log.info("Shutting down gRPC server...");
            server.shutdown().awaitTermination(15, TimeUnit.SECONDS);
            log.info("gRPC server shut down complete");
        }
    }
}
```

---

### 3. Event Publisher

#### kafka/message/EventEnvelope.java
Serializes Protobuf payload as inline readable JSON.
```java
package com.gym.common.kafka.message;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = EventEnvelopeSerializer.class)
public record EventEnvelope<T extends com.google.protobuf.Message>(
    String eventType,
    String key,
    T payload,
    long timestamp,
    String traceId,
    String source
) {}
```

#### kafka/message/EventEnvelopeSerializer.java
Converts protobuf messages to human-readable JSON segments using standard utility parser inside envelope.
```java
package com.gym.common.kafka.message;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;

public class EventEnvelopeSerializer extends JsonSerializer<EventEnvelope<?>> {
    @Override
    public void serialize(EventEnvelope<?> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("event_type", value.eventType());
        gen.writeStringField("key", value.key());
        gen.writeNumberField("timestamp", value.timestamp());
        gen.writeStringField("trace_id", value.traceId());
        gen.writeStringField("source", value.source());
        
        gen.writeFieldName("payload");
        if (value.payload() != null) {
            String jsonPayload = JsonFormat.printer()
                .omittingInsignificantWhitespace()
                .print(value.payload());
            gen.writeRawValue(jsonPayload);
        } else {
            gen.writeNull();
        }
        gen.writeEndObject();
    }
}
```

#### kafka/producer/EventPublisher.java
```java
package com.gym.common.kafka.producer;

import com.google.protobuf.Message;
import java.util.Map;

public interface EventPublisher {
    void publish(String topic, String key, Message payload);
    void publish(String topic, String key, Message payload, Map<String, String> headers);
}
```

#### kafka/producer/EventPublisherImpl.java
Injects telemetry context tracing. Serializes metadata into records.
```java
package com.gym.common.kafka.producer;

import com.google.protobuf.Message;
import com.gym.common.kafka.message.EventEnvelope;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

@Component
public class EventPublisherImpl implements EventPublisher {
    private static final Logger log = LoggerFactory.getLogger(EventPublisherImpl.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String applicationName;

    public EventPublisherImpl(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${spring.application.name:unknown-service}") String applicationName) {
        this.kafkaTemplate = kafkaTemplate;
        this.applicationName = applicationName;
    }

    @Override
    public void publish(String topic, String key, Message payload) {
        publish(topic, key, payload, Map.of());
    }

    @Override
    public void publish(String topic, String key, Message payload, Map<String, String> headers) {
        String traceId = Span.current().getSpanContext().getTraceId();
        long timestamp = Instant.now().toEpochMilli();
        String eventType = payload.getClass().getSimpleName();

        EventEnvelope<Message> envelope = new EventEnvelope<>(
                eventType,
                key,
                payload,
                timestamp,
                traceId,
                applicationName
        );

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, envelope);
        
        record.headers().add(new RecordHeader("x-event-type", eventType.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("x-trace-id", traceId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("x-source", applicationName.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("x-timestamp", String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8)));

        headers.forEach((k, v) -> {
            if (v != null) {
                record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
            }
        });

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event to topic: {} with key: {}", topic, key, ex);
            } else {
                log.debug("Event sent to topic: {}, partition: {}", topic, result.getRecordMetadata().partition());
            }
        });
    }
}
```

---

### 4. Retryable Consumer & DLQ Routing

Uses Spring Kafka container-level listener error handlers. Retries execution. If error persists, publishes to DLQ.

#### kafka/config/KafkaAutoConfig.java
Configures error handler. Targets `[originalTopic].DLQ`. Calculates headers. Retries 3x with exponential spacing.
```java
package com.gym.common.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
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
public class KafkaAutoConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

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
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.gym.*");
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLQ", record.partition()));

        recoverer.setHeadersFunction((record, exception) -> {
            Headers headers = new RecordHeaders();
            headers.add("x-original-topic", record.topic().getBytes(StandardCharsets.UTF_8));
            headers.add("x-exception-message", exception.getCause() != null 
                    ? exception.getCause().getMessage().getBytes(StandardCharsets.UTF_8)
                    : exception.getMessage().getBytes(StandardCharsets.UTF_8));
            headers.add("x-failed-at", String.valueOf(Instant.now().toEpochMilli()).getBytes(StandardCharsets.UTF_8));
            
            int attempt = 1;
            org.apache.kafka.common.header.Header countHeader = record.headers().lastHeader("x-retry-count");
            if (countHeader != null) {
                attempt = Integer.parseInt(new String(countHeader.value(), StandardCharsets.UTF_8)) + 1;
            }
            headers.add("x-retry-count", String.valueOf(attempt).getBytes(StandardCharsets.UTF_8));
            return headers;
        });

        // 3 retries (total 4 attempts)
        // Exponential backoff: 2s, 4s, 8s
        ExponentialBackOff backOff = new ExponentialBackOff(2000L, 2.0);
        backOff.setMaxInterval(8000L);
        backOff.setMaxElapsedTime(15000L);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
```

#### kafka/consumer/RetryableConsumer.java
Abstract helper class. Enforces simple signature contract. Throws failures up to container factory.
```java
package com.gym.common.kafka.consumer;

import com.google.protobuf.Message;
import com.gym.common.kafka.message.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RetryableConsumer<T extends Message> {
    private static final Logger log = LoggerFactory.getLogger(RetryableConsumer.class);

    public abstract void onMessage(EventEnvelope<T> envelope) throws Exception;

    protected void handleProcessingError(EventEnvelope<T> envelope, Exception ex) {
        log.error("Failed to process event type {} with key {}: {}", envelope.eventType(), envelope.key(), ex.getMessage(), ex);
        throw new RuntimeException("Consumer error, triggering retry/DLQ", ex);
    }
}
```

### Critical Files for Implementation
List 3-5 files most critical for implementing this plan:
- /home/phucl/Workplace/gapi/common-java/build.gradle
- /home/phucl/Workplace/gapi/common-java/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
- /home/phucl/Workplace/gapi/common-java/src/main/java/com/gym/common/grpc/config/GrpcServerAutoConfig.java
- /home/phucl/Workplace/gapi/common-java/src/main/java/com/gym/common/kafka/config/KafkaAutoConfig.java
- /home/phucl/Workplace/gapi/common-java/src/main/java/com/gym/common/grpc/interceptor/AuthServerInterceptor.java
