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
