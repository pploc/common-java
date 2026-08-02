package com.gym.common.kafka.producer;

import com.google.protobuf.Message;
import com.gym.common.error.EventPublishFailedException;
import com.gym.common.kafka.config.KafkaEventProperties;
import io.opentelemetry.api.trace.Span;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class EventPublisherImpl implements EventPublisher {

    public static final String HEADER_EVENT_TYPE = "event-type";
    public static final String HEADER_SOURCE = "source";
    public static final String HEADER_TIMESTAMP = "timestamp";
    public static final String HEADER_EVENT_ID = "event-id";
    public static final String HEADER_TRACEPARENT = "traceparent";
    public static final String HEADER_TRACESTATE = "tracestate";
    public static final String HEADER_TRACE_ID = "x-trace-id";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String applicationName;
    private final KafkaEventProperties kafkaEventProperties;
    private final Clock clock;

    public EventPublisherImpl(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${spring.application.name:unknown-service}") String applicationName,
            KafkaEventProperties kafkaEventProperties) {
        this(kafkaTemplate, applicationName, kafkaEventProperties, Clock.systemUTC());
    }

    public EventPublisherImpl(
            KafkaTemplate<String, Object> kafkaTemplate,
            String applicationName,
            KafkaEventProperties kafkaEventProperties,
            Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.applicationName = applicationName;
        this.kafkaEventProperties = kafkaEventProperties;
        this.clock = clock != null ? clock : Clock.systemUTC();
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

        var spanContext = Span.current().getSpanContext();
        long timestamp = Instant.now(clock).toEpochMilli();
        String eventType = payload.getDescriptorForType().getFullName();

        // A Protobuf Kafka serializer configured by KafkaAutoConfig applies the
        // Schema Registry framing. The transport value is never a JSON envelope.
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, payload);

        record.headers().add(new RecordHeader(HEADER_EVENT_TYPE, eventType.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(HEADER_SOURCE, applicationName.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(HEADER_TIMESTAMP, String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(HEADER_EVENT_ID, eventId.getBytes(StandardCharsets.UTF_8)));
        if (spanContext.isValid()) {
            String traceparent = String.format(
                    "00-%s-%s-%02x",
                    spanContext.getTraceId(),
                    spanContext.getSpanId(),
                    spanContext.getTraceFlags().asByte());
            record.headers().add(new RecordHeader(HEADER_TRACEPARENT, traceparent.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(HEADER_TRACE_ID, spanContext.getTraceId().getBytes(StandardCharsets.UTF_8)));
        }

        headers.forEach((k, v) -> {
            if (v != null) {
                record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
            }
        });

        long timeoutMs = kafkaEventProperties.getPublishTimeout().toMillis();
        try {
            kafkaTemplate.send(record).get(timeoutMs, TimeUnit.MILLISECONDS);
            log.info("Published event type {} to {} with event ID {}", eventType, topic, eventId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw EventPublishFailedException.of(eventType, e);
        } catch (ExecutionException | TimeoutException e) {
            throw EventPublishFailedException.of(eventType, e);
        }
    }
}

