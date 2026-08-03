package com.gym.common.kafka.consumer;

import java.util.Objects;

/** Creates one explicit raw delivery coordinator for a service-owned handler. */
public final class RawDeliveryCoordinatorFactory {
    private final RawKafkaDecoder decoder;
    private final RawDlqPublisher dlqPublisher;
    private final DeliverySleeper sleeper;

    public RawDeliveryCoordinatorFactory(RawKafkaDecoder decoder, RawDlqPublisher dlqPublisher, DeliverySleeper sleeper) {
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.dlqPublisher = Objects.requireNonNull(dlqPublisher, "dlqPublisher");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    public RawDeliveryCoordinator forHandler(KafkaRecordHandler handler) {
        return new RawDeliveryCoordinator(decoder, handler, dlqPublisher, sleeper);
    }
}
