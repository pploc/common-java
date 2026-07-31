package com.gym.common.kafka.producer;

import com.google.protobuf.Message;
import java.util.Map;

public interface EventPublisher {
    void publish(String topic, String key, Message payload);
    void publish(String topic, String key, Message payload, Map<String, String> headers);
}
