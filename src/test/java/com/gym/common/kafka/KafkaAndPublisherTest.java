package com.gym.common.kafka;

import com.google.protobuf.Message;
import com.gym.common.error.EventPublishFailedException;
import com.gym.common.kafka.config.KafkaEventProperties;
import com.gym.common.kafka.producer.EventPublisherImpl;
import com.gym.proto.events.v1.UserRegisteredEvent;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAndPublisherTest {
    private static final String TOPIC = "identity.user.registered.v1";
    private static final UserRegisteredEvent EVENT = UserRegisteredEvent.getDefaultInstance();

    @Test
    void givenFrozenProtobufEvent_whenPublishing_thenAddsCanonicalHeaders() {
        KafkaTemplate<String, Message> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventProperties properties = new KafkaEventProperties();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_700_000_000_123L), ZoneOffset.UTC);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", properties, clock);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successfulSend());

        publisher.publish(TOPIC, "key-1", EVENT, "event-1", Map.of("x-custom", "val"));

        ProducerRecord<String, Message> record = capturedRecord(kafkaTemplate);
        assertSame(EVENT, record.value());
        assertEquals("events.v1.UserRegisteredEvent", header(record, EventPublisherImpl.HEADER_EVENT_TYPE));
        assertEquals("gym-service", header(record, EventPublisherImpl.HEADER_SOURCE));
        assertEquals("1700000000123", header(record, EventPublisherImpl.HEADER_TIMESTAMP));
        assertEquals("event-1", header(record, EventPublisherImpl.HEADER_EVENT_ID));
        assertNotNull(record.headers().lastHeader(EventPublisherImpl.HEADER_TRACEPARENT));
        assertEquals("val", header(record, "x-custom"));
    }

    @Test
    void givenValidW3CContext_whenPublishing_thenPropagatesW3CHeaders() {
        KafkaTemplate<String, Message> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successfulSend());

        SpanContext spanContext = SpanContext.create(
                "0af7651916cd43dd8448eb211c80319c",
                "b7ad6b7169203331",
                TraceFlags.getSampled(),
                TraceState.builder().put("vendor", "value").build()
        );
        try (Scope ignored = Span.wrap(spanContext).storeInContext(Context.current()).makeCurrent()) {
            publisher.publish(TOPIC, "key-1", EVENT, "event-1", Map.of());
        }

        ProducerRecord<String, Message> record = capturedRecord(kafkaTemplate);
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                header(record, EventPublisherImpl.HEADER_TRACEPARENT));
        assertEquals("vendor=value", header(record, EventPublisherImpl.HEADER_TRACESTATE));
        assertEquals(null, record.headers().lastHeader(EventPublisherImpl.HEADER_TRACE_ID));
    }

    @Test
    void givenCorrelationFallback_whenPublishing_thenRetainsItWithoutReplacingW3CHeader() {
        KafkaTemplate<String, Message> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successfulSend());

        publisher.publish(TOPIC, "key-1", EVENT, "event-1", Map.of(EventPublisherImpl.HEADER_TRACE_ID, "legacy-trace"));

        ProducerRecord<String, Message> record = capturedRecord(kafkaTemplate);
        assertEquals("legacy-trace", header(record, EventPublisherImpl.HEADER_TRACE_ID));
        assertNotNull(record.headers().lastHeader(EventPublisherImpl.HEADER_TRACEPARENT));
    }

    @Test
    void givenReservedHeadersOrUnknownPair_whenPublishing_thenRejectsRequest() {
        KafkaTemplate<String, Message> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());

        assertThrows(IllegalArgumentException.class, () -> publisher.publish(
                TOPIC, "key-1", EVENT, "event-1", Map.of(EventPublisherImpl.HEADER_SOURCE, "spoofed")
        ));
        assertThrows(IllegalArgumentException.class, () -> publisher.publish(
                "test-topic", "key-1", EVENT, "event-1", Map.of()
        ));
    }

    @Test
    void givenInvalidValuesOrRegistrationEnabled_whenPublishing_thenRejectsRequest() {
        KafkaTemplate<String, Message> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());

        assertThrows(NullPointerException.class, () -> publisher.publish(null, "key", EVENT));
        assertThrows(NullPointerException.class, () -> publisher.publish(TOPIC, "key", null));
        assertThrows(IllegalArgumentException.class, () -> publisher.publish(" ", "key", EVENT));
        assertThrows(IllegalArgumentException.class, () -> publisher.publish(TOPIC, "key", EVENT, " ", Map.of()));

        KafkaEventProperties autoRegistration = new KafkaEventProperties();
        autoRegistration.setAutoRegisterSchemas(true);
        EventPublisherImpl blocked = new EventPublisherImpl(kafkaTemplate, "gym-service", autoRegistration);
        assertThrows(IllegalStateException.class, () -> blocked.publish(TOPIC, "key", EVENT));
    }

    @Test
    void givenBrokerAcknowledgementFailure_whenPublishing_thenRaisesPublishFailure() {
        KafkaTemplate<String, Message> kafkaTemplate = mock(KafkaTemplate.class);
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", new KafkaEventProperties());
        CompletableFuture<SendResult<String, Message>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka unreachable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        assertThrows(EventPublishFailedException.class, () -> publisher.publish(TOPIC, "key-1", EVENT));
    }

    private static CompletableFuture<SendResult<String, Message>> successfulSend() {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(TOPIC, 0), 0, 0, 0, 0, 0);
        return CompletableFuture.completedFuture(new SendResult<>(null, metadata));
    }

    @SuppressWarnings("unchecked")
    private static ProducerRecord<String, Message> capturedRecord(KafkaTemplate<String, Message> kafkaTemplate) {
        var captor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        return captor.getValue();
    }

    private static String header(ProducerRecord<String, Message> record, String key) {
        assertNotNull(record.headers().lastHeader(key), "missing header: " + key);
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
