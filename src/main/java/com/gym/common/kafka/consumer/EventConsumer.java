package com.gym.common.kafka.consumer;

import com.google.protobuf.Message;
import com.gym.common.kafka.message.EventEnvelope;

/**
 * Legacy JSON-envelope consumer adapter. New Kafka listeners consume concrete
 * Protobuf messages configured by {@code KafkaAutoConfig}.
 */
public interface EventConsumer<T extends Message> {
    void onMessage(EventEnvelope<T> envelope) throws Exception;
}
