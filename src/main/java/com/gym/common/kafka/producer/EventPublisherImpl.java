package com.gym.common.kafka.producer;

import com.google.protobuf.Message;
import com.gym.common.error.EventPublishFailedException;
import com.gym.common.kafka.KafkaContract;
import com.gym.common.kafka.config.KafkaEventProperties;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Acknowledged concrete-Protobuf publisher for the frozen v1 topic set. */
@Slf4j
@Component
public class EventPublisherImpl implements EventPublisher {
    public static final String HEADER_EVENT_TYPE = KafkaContract.HEADER_EVENT_TYPE;
    public static final String HEADER_SOURCE = KafkaContract.HEADER_SOURCE;
    public static final String HEADER_TIMESTAMP = KafkaContract.HEADER_TIMESTAMP;
    public static final String HEADER_EVENT_ID = KafkaContract.HEADER_EVENT_ID;
    public static final String HEADER_TRACEPARENT = KafkaContract.HEADER_TRACEPARENT;
    public static final String HEADER_TRACESTATE = KafkaContract.HEADER_TRACESTATE;
    public static final String HEADER_TRACE_ID = KafkaContract.HEADER_TRACE_ID;

    private final KafkaTemplate<String, Message> kafkaTemplate;
    private final String applicationName;
    private final KafkaEventProperties kafkaEventProperties;
    private final Clock clock;

    @Autowired
    public EventPublisherImpl(KafkaTemplate<String, Message> kafkaTemplate,
                              @Value("${spring.application.name:unknown-service}") String applicationName,
                              KafkaEventProperties kafkaEventProperties) {
        this(kafkaTemplate, applicationName, kafkaEventProperties, Clock.systemUTC());
    }

    public EventPublisherImpl(KafkaTemplate<String, Message> kafkaTemplate, String applicationName,
                              KafkaEventProperties kafkaEventProperties, Clock clock) {
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate");
        this.applicationName = applicationName;
        this.kafkaEventProperties = Objects.requireNonNull(kafkaEventProperties, "kafkaEventProperties");
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    @Override
    public void publish(String topic, String key, Message payload) {
        publish(topic, key, payload, Map.of());
    }

    @Override
    public void publish(String topic, String key, Message payload, Map<String, String> headers) {
        publish(topic, key, payload, UUID.randomUUID().toString(), headers);
    }

    @Override
    public void publish(String topic, String key, Message payload, String eventId, Map<String, String> headers) {
        Objects.requireNonNull(topic, "Topic cannot be null");
        Objects.requireNonNull(payload, "Payload cannot be null");
        Objects.requireNonNull(eventId, "Event ID cannot be null");
        Objects.requireNonNull(headers, "Headers cannot be null");
        validate(topic, payload, eventId);

        ProducerRecord<String, Message> record = new ProducerRecord<>(topic, key, payload);
        add(record, HEADER_EVENT_TYPE, payload.getDescriptorForType().getFullName());
        add(record, HEADER_SOURCE, applicationName.trim());
        add(record, HEADER_TIMESTAMP, Long.toString(Instant.now(clock).toEpochMilli()));
        add(record, HEADER_EVENT_ID, eventId.trim());
        injectTrace(record, headers);
        headers.forEach((headerKey, value) -> {
            if (!HEADER_TRACE_ID.equalsIgnoreCase(headerKey)) {
                addCallerHeader(record, headerKey, value);
            }
        });

        try {
            kafkaTemplate.send(record).get(kafkaEventProperties.getPublishTimeout().toMillis(), TimeUnit.MILLISECONDS);
            log.info("Published Kafka event: type={}, topic={}", payload.getDescriptorForType().getFullName(), topic);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw EventPublishFailedException.of(payload.getDescriptorForType().getFullName(), exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw EventPublishFailedException.of(payload.getDescriptorForType().getFullName(), exception);
        }
    }

    private void validate(String topic, Message payload, String eventId) {
        if (topic.isBlank() || applicationName == null || applicationName.isBlank() || eventId.isBlank()) {
            throw new IllegalArgumentException("Kafka topic, source, and event ID are required");
        }
        KafkaContract.requireFrozenPair(topic, payload);
        KafkaContract.requireValid(payload);
        if (kafkaEventProperties.isAutoRegisterSchemas()) {
            throw new IllegalStateException("Production Kafka publishing must disable schema auto-registration");
        }
    }

    private static void add(ProducerRecord<String, Message> record, String key, String value) {
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    private static void addCallerHeader(ProducerRecord<String, Message> record, String headerKey, String value) {
        if (headerKey == null) {
            throw new IllegalArgumentException("Kafka header name cannot be null");
        }
        if (KafkaContract.RESERVED_HEADERS.contains(headerKey.trim().toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Caller cannot override canonical Kafka header: " + headerKey);
        }
        if (value != null) {
            add(record, headerKey, value);
        }
    }

    private static void injectTrace(ProducerRecord<String, Message> record, Map<String, String> headers) {
        SpanContext spanContext = Span.current().getSpanContext();
        if (spanContext.isValid()) {
            W3CTraceContextPropagator.getInstance().inject(Context.current(), record.headers(),
                    (carrier, key, value) -> carrier.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8))));
        } else {
            add(record, HEADER_TRACEPARENT, newRootTraceparent());
            String fallback = headers.get(HEADER_TRACE_ID);
            if (fallback != null && !fallback.isBlank()) {
                add(record, HEADER_TRACE_ID, fallback.trim());
            }
        }
    }

    private static String newRootTraceparent() {
        String traceId = UUID.randomUUID().toString().replace("-", "");
        String spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        return "00-" + traceId + "-" + spanId + "-01";
    }
}
