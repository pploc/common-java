package com.gym.common.kafka.consumer;

import com.google.protobuf.Message;
import com.gym.common.kafka.message.EventEnvelope;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RetryableConsumer<T extends Message> {

    public abstract void onMessage(EventEnvelope<T> envelope) throws Exception;

    protected void handleProcessingError(EventEnvelope<T> envelope, Exception ex) {
        log.error("Failed to process event type {} with key {}: {}", envelope.eventType(), envelope.key(), ex.getMessage(), ex);
        throw new RuntimeException("Consumer error, triggering retry/DLQ", ex);
    }
}

