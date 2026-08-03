package com.gym.common.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Objects;

/** Bridges Spring's raw listener callback to the library-owned delivery state machine. */
public final class RawKafkaListenerAdapter {
    private final RawDeliveryCoordinator coordinator;

    public RawKafkaListenerAdapter(RawDeliveryCoordinator coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    public void deliver(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment) {
        coordinator.deliver(RawKafkaRecord.from(Objects.requireNonNull(record, "record")),
                Objects.requireNonNull(acknowledgment, "acknowledgment"));
    }
}
