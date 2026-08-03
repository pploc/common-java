package com.gym.common.kafka.consumer;

/** Decodes and validates a raw Confluent-framed record without discarding it. */
@FunctionalInterface
public interface RawKafkaDecoder {
    DecodedKafkaRecord decode(RawKafkaRecord record);
}
