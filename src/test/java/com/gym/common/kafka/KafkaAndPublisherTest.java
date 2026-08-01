package com.gym.common.kafka;

import com.google.protobuf.Empty;
import com.gym.common.kafka.config.KafkaEventProperties;
import com.gym.common.kafka.consumer.RetryableConsumer;
import com.gym.common.kafka.message.EventEnvelope;
import com.gym.common.kafka.producer.EventPublisherImpl;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaAndPublisherTest {

    @Test
    void testEventPublisherImplPublishSuccess() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventProperties properties = new KafkaEventProperties();
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", properties);

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("test-topic", 0), 0, 0, 0, 0, 0);
        SendResult<String, Object> sendResult = new SendResult<>(null, metadata);
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        publisher.publish("test-topic", "key-1", Empty.getDefaultInstance());
        verify(kafkaTemplate).send(any(ProducerRecord.class));

        publisher.publish("test-topic", "key-1", Empty.getDefaultInstance(), Map.of("x-custom", "val"));
    }

    @Test
    void testEventPublisherNullValidations() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventProperties properties = new KafkaEventProperties();
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", properties);

        assertThrows(NullPointerException.class, () -> publisher.publish(null, "key", Empty.getDefaultInstance()));
        assertThrows(NullPointerException.class, () -> publisher.publish("topic", "key", null));
    }

    @Test
    void testRetryableConsumer() {
        RetryableConsumer<Empty> consumer = new RetryableConsumer<>() {
            @Override
            public void onMessage(EventEnvelope<Empty> envelope) throws Exception {
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
        KafkaEventProperties properties = new KafkaEventProperties();
        EventPublisherImpl publisher = new EventPublisherImpl(kafkaTemplate, "gym-service", properties);

        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka unreachable"));

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        assertThrows(com.gym.common.error.EventPublishFailedException.class,
                () -> publisher.publish("test-topic", "key-1", Empty.getDefaultInstance()));
    }
}
