package com.gym.common.kafka;

import com.gym.common.kafka.consumer.RawKafkaDlqPublisher;
import com.gym.common.kafka.consumer.RawKafkaHeader;
import com.gym.common.kafka.consumer.RawKafkaRecord;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RawKafkaDlqPublisherTest {
    @Test
    void givenRawRecord_whenPublishingDlq_thenPreservesBytesAndReplacesFrozenHeaders() throws Exception {
        KafkaTemplate<byte[], byte[]> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(success());
        RawKafkaDlqPublisher publisher = new RawKafkaDlqPublisher(
                kafkaTemplate, ".DLQ", 1_000, Clock.fixed(Instant.ofEpochMilli(1700000000123L), ZoneOffset.UTC)
        );
        RawKafkaRecord source = new RawKafkaRecord(
                "identity.user.registered.v1", 2, 4L, new byte[]{1}, new byte[]{2, 3}, List.of(
                        new RawKafkaHeader("x-custom", new byte[]{4}),
                        new RawKafkaHeader(KafkaContract.HEADER_RETRY_COUNT, bytes("old")),
                        new RawKafkaHeader(KafkaContract.HEADER_ORIGINAL_TOPIC, bytes("old-topic"))
                )
        );

        publisher.publish(source, "Kafka message processing failed after retries", 4);

        ProducerRecord<byte[], byte[]> record = captured(kafkaTemplate);
        assertEquals("identity.user.registered.v1.DLQ", record.topic());
        assertArrayEquals(new byte[]{1}, record.key());
        assertArrayEquals(new byte[]{2, 3}, record.value());
        assertArrayEquals(new byte[]{4}, record.headers().lastHeader("x-custom").value());
        assertEquals("identity.user.registered.v1", header(record, KafkaContract.HEADER_ORIGINAL_TOPIC));
        assertEquals("Kafka message processing failed after retries", header(record, KafkaContract.HEADER_EXCEPTION_MESSAGE));
        assertEquals("1700000000123", header(record, KafkaContract.HEADER_FAILED_AT));
        assertEquals("4", header(record, KafkaContract.HEADER_RETRY_COUNT));
    }

    @Test
    void givenBrokerFailure_whenPublishingDlq_thenPropagatesFailureWithoutAcknowledgement() {
        KafkaTemplate<byte[], byte[]> kafkaTemplate = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<byte[], byte[]>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);
        RawKafkaDlqPublisher publisher = new RawKafkaDlqPublisher(kafkaTemplate, ".DLQ", 1_000, Clock.systemUTC());

        assertThrows(Exception.class, () -> publisher.publish(source(), "failed", 1));
    }

    private static RawKafkaRecord source() {
        return new RawKafkaRecord("identity.user.registered.v1", 0, 1L, new byte[]{1}, new byte[]{2}, List.of());
    }

    private static CompletableFuture<SendResult<byte[], byte[]>> success() {
        return CompletableFuture.completedFuture(new SendResult<>(null,
                new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0, 0, 0)));
    }

    @SuppressWarnings("unchecked")
    private static ProducerRecord<byte[], byte[]> captured(KafkaTemplate<byte[], byte[]> template) {
        var captor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        return captor.getValue();
    }

    private static String header(ProducerRecord<byte[], byte[]> record, String key) {
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
