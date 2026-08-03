package com.gym.common.kafka.consumer;

import java.util.Arrays;
import java.util.Objects;

/** Header bytes copied directly from a Kafka record without normalization. */
public record RawKafkaHeader(String key, byte[] value) {
    public RawKafkaHeader {
        Objects.requireNonNull(key, "key");
        value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }
}
