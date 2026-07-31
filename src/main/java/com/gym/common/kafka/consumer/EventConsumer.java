package com.gym.common.kafka.consumer;

import com.google.protobuf.Message;
import com.gym.common.kafka.message.EventEnvelope;

public interface EventConsumer<T extends Message> {
    void onMessage(EventEnvelope<T> envelope) throws Exception;
}
