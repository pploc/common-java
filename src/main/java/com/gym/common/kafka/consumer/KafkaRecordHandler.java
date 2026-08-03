package com.gym.common.kafka.consumer;

/** Handles one validated concrete-Protobuf delivery. */
@FunctionalInterface
public interface KafkaRecordHandler {
    void handle(DecodedKafkaRecord record) throws Exception;
}
