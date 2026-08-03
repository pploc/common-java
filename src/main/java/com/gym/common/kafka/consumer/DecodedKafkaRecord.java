package com.gym.common.kafka.consumer;

import com.google.protobuf.Message;

import java.util.Objects;

/** A concrete Protobuf message paired with its untouched broker bytes. */
public record DecodedKafkaRecord(RawKafkaRecord raw, Message message) {
    public DecodedKafkaRecord {
        Objects.requireNonNull(raw, "raw");
        Objects.requireNonNull(message, "message");
    }
}
