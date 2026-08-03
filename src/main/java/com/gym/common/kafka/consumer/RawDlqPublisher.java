package com.gym.common.kafka.consumer;

/** Publishes a raw failed record and returns only after the broker acknowledges it. */
@FunctionalInterface
public interface RawDlqPublisher {
    void publish(RawKafkaRecord record, String diagnostic, int attempts) throws Exception;
}
