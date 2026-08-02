package com.gym.common.kafka;

import com.google.protobuf.Empty;
import com.gym.common.error.EventPublishFailedException;
import com.gym.common.kafka.config.KafkaEventProperties;
import com.gym.common.kafka.consumer.RetryableConsumer;
import com.gym.common.kafka.message.EventEnvelope;
import com.gym.common.kafka.producer.EventPublisherImpl;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAndPublisherTest {

    @Test
    void testEventPublisherImplPublishesRawProtobufWithCanonicalHeaders() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventProperties properties = new KafkaEventProperties();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_700_000_000_123L), ZoneOffset.UTC);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", properties, clock);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successfulSend());

        publisher.publish("test-topic", "key-1", Empty.getDefaultInstance(), "event-1", Map.of("x-custom", "val"));

        ProducerRecord<String, Object> record = capturedRecord(kafkaTemplate);
        assertSame(Empty.getDefaultInstance(), record.value());
        assertEquals("google.protobuf.Empty", header(record, EventPublisherImpl.HEADER_EVENT_TYPE));
        assertEquals("gym-service", header(record, EventPublisherImpl.HEADER_SOURCE));
        assertEquals("1700000000123", header(record, EventPublisherImpl.HEADER_TIMESTAMP));
        assertEquals("event-1", header(record, EventPublisherImpl.HEADER_EVENT_ID));
        assertEquals("val", header(record, "x-custom"));
        assertEquals(null, record.headers().lastHeader("x-event-type"));
    }

    @Test
    void testEventPublisherInjectsW3CTraceContext() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successfulSend());

        SpanContext spanContext = SpanContext.create(
                "0af7651916cd43dd8448eb211c80319c",
                "b7ad6b7169203331",
                TraceFlags.getSampled(),
                TraceState.builder().put("vendor", "value").build()
        );
        try (Scope ignored = Span.wrap(spanContext).storeInContext(Context.current()).makeCurrent()) {
            publisher.publish(
                    "test-topic",
                    "key-1",
                    Empty.getDefaultInstance(),
                    "event-1",
                    Map.of()
            );
        }

        ProducerRecord<String, Object> record = capturedRecord(kafkaTemplate);
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                header(record, EventPublisherImpl.HEADER_TRACEPARENT));
        assertEquals("vendor=value", header(record, EventPublisherImpl.HEADER_TRACESTATE));
        assertEquals(null, record.headers().lastHeader(EventPublisherImpl.HEADER_TRACE_ID));
    }

    @Test
    void testEventPublisherUsesTraceIdOnlyWithoutW3CContext() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successfulSend());

        publisher.publish(
                "test-topic",
                "key-1",
                Empty.getDefaultInstance(),
                "event-1",
                Map.of(EventPublisherImpl.HEADER_TRACE_ID, "legacy-trace")
        );

        ProducerRecord<String, Object> record = capturedRecord(kafkaTemplate);
        assertEquals("legacy-trace", header(record, EventPublisherImpl.HEADER_TRACE_ID));
        assertEquals(null, record.headers().lastHeader(EventPublisherImpl.HEADER_TRACEPARENT));
    }

    @Test
    void testEventPublisherRejectsCanonicalHeaderOverride() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());

        assertThrows(IllegalArgumentException.class, () -> publisher.publish(
                "test-topic", "key-1", Empty.getDefaultInstance(), "event-1",
                Map.of(EventPublisherImpl.HEADER_SOURCE, "spoofed")
        ));
    }

    @Test
    void testEventPublisherValidatesRequiredValues() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());

        assertThrows(NullPointerException.class, () -> publisher.publish(null, "key", Empty.getDefaultInstance()));
        assertThrows(NullPointerException.class, () -> publisher.publish("topic", "key", null));
        assertThrows(IllegalArgumentException.class,
                () -> publisher.publish(" ", "key", Empty.getDefaultInstance()));
        assertThrows(IllegalArgumentException.class,
                () -> publisher.publish("topic", "key", Empty.getDefaultInstance(), " ", Map.of()));
    }

    @Test
    void testRetryableConsumer() {
        RetryableConsumer<Empty> consumer = new RetryableConsumer<>() {
            @Override
            public void onMessage(EventEnvelope<Empty> envelope) {
                if ("error".equals(envelope.key())) {
                    throw new IllegalArgumentException("Test processing error");
                }
            }
        };

        EventEnvelope<Empty> validEnvelope = new EventEnvelope<>("EmptyEvent", "valid", Empty.getDefaultInstance(), 0L, "t", "s");
        assertDoesNotThrow(() -> consumer.onMessage(validEnvelope));

        EventEnvelope<Empty> errorEnvelope = new EventEnvelope<>("EmptyEvent", "error", Empty.getDefaultInstance(), 0L, "t", "s");
        assertThrows(IllegalArgumentException.class, () -> consumer.onMessage(errorEnvelope));
    }

    @Test
    void testEventPublisherImplPublishFailure() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());
        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka unreachable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        assertThrows(EventPublishFailedException.class,
                () -> publisher.publish("test-topic", "key-1", Empty.getDefaultInstance()));
    }

    private static CompletableFuture<SendResult<String, Object>> successfulSend() {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("test-topic", 0), 0, 0, 0, 0, 0);
        return CompletableFuture.completedFuture(new SendResult<>(null, metadata));
    }

    @SuppressWarnings("unchecked")
    private static ProducerRecord<String, Object> capturedRecord(KafkaTemplate<String, Object> kafkaTemplate) {
        var captor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        return captor.getValue();
    }

    private static String header(ProducerRecord<String, Object> record, String key) {
        assertNotNull(record.headers().lastHeader(key), "missing header: " + key);
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
