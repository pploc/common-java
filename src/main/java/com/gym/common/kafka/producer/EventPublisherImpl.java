package com.gym.common.kafka.producer;

import com.google.protobuf.Message;
import com.gym.common.error.EventPublishFailedException;
import com.gym.common.kafka.message.EventEnvelope;
import com.gym.common.kafka.config.KafkaEventProperties;
import io.opentelemetry.api.trace.Span;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class EventPublisherImpl implements EventPublisher {

    public static final String HEADER_EVENT_TYPE = "x-event-type";
    public static final String HEADER_TRACE_ID = "x-trace-id";
    public static final String HEADER_SOURCE = "x-source";
    public static final String HEADER_TIMESTAMP = "x-timestamp";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String applicationName;
    private final KafkaEventProperties kafkaEventProperties;

    public EventPublisherImpl(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${spring.application.name:unknown-service}") String applicationName,
            KafkaEventProperties kafkaEventProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.applicationName = applicationName;
        this.kafkaEventProperties = kafkaEventProperties;
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

        record.headers().add(new RecordHeader(HEADER_EVENT_TYPE, eventType.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(HEADER_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(HEADER_SOURCE, applicationName.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(HEADER_TIMESTAMP, String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8)));

        headers.forEach((k, v) -> {
            if (v != null) {
                record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
            }
        });

        long timeoutMs = kafkaEventProperties.getPublishTimeout().toMillis();
        try {
            kafkaTemplate.send(record).get(timeoutMs, TimeUnit.MILLISECONDS);
            log.info("Published event type {} to {}", eventType, topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw EventPublishFailedException.of(eventType, e);
        } catch (ExecutionException | TimeoutException e) {
            throw EventPublishFailedException.of(eventType, e);
        }
    }
}

