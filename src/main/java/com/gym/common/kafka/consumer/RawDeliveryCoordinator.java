package com.gym.common.kafka.consumer;

import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Processes one record synchronously. The listener container invokes records in
 * partition order; this coordinator acknowledges only after success or an
 * acknowledged byte-preserving DLQ publication.
 */
public final class RawDeliveryCoordinator {
    private static final List<Duration> RETRY_DELAYS = List.of(
            Duration.ofSeconds(2), Duration.ofSeconds(4), Duration.ofSeconds(8)
    );
    private static final String PERMANENT_DIAGNOSTIC = "Kafka message is permanently invalid";
    private static final String RETRY_EXHAUSTED_DIAGNOSTIC = "Kafka message processing failed after retries";

    private final RawKafkaDecoder decoder;
    private final KafkaRecordHandler handler;
    private final RawDlqPublisher dlqPublisher;
    private final DeliverySleeper sleeper;

    public RawDeliveryCoordinator(
            RawKafkaDecoder decoder,
            KafkaRecordHandler handler,
            RawDlqPublisher dlqPublisher,
            DeliverySleeper sleeper
    ) {
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.dlqPublisher = Objects.requireNonNull(dlqPublisher, "dlqPublisher");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    public void deliver(RawKafkaRecord raw, Acknowledgment acknowledgment) {
        Objects.requireNonNull(raw, "raw");
        Objects.requireNonNull(acknowledgment, "acknowledgment");
        try {
            processWithRetries(raw);
            acknowledgment.acknowledge();
        } catch (DeliveryFailure failure) {
            publishDlqThenAcknowledge(raw, failure.diagnostic, failure.attempts, acknowledgment, failure.getCause());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka delivery was interrupted before source acknowledgement", exception);
        }
    }

    private void processWithRetries(RawKafkaRecord raw) throws InterruptedException {
        int attempts = 1;
        try {
            process(raw);
            return;
        } catch (PermanentKafkaException exception) {
            throw new DeliveryFailure(PERMANENT_DIAGNOSTIC, attempts, exception);
        } catch (Exception initialFailure) {
            Exception lastFailure = initialFailure;
            for (Duration delay : RETRY_DELAYS) {
                sleeper.sleep(delay);
                attempts++;
                try {
                    process(raw);
                    return;
                } catch (PermanentKafkaException exception) {
                    throw new DeliveryFailure(PERMANENT_DIAGNOSTIC, attempts, exception);
                } catch (Exception retryFailure) {
                    lastFailure = retryFailure;
                }
            }
            throw new DeliveryFailure(RETRY_EXHAUSTED_DIAGNOSTIC, attempts, lastFailure);
        }
    }

    private void process(RawKafkaRecord raw) throws Exception {
        handler.handle(decoder.decode(raw));
    }

    private void publishDlqThenAcknowledge(
            RawKafkaRecord raw,
            String diagnostic,
            int attempts,
            Acknowledgment acknowledgment,
            Throwable failure
    ) {
        try {
            dlqPublisher.publish(raw, diagnostic, attempts);
            acknowledgment.acknowledge();
        } catch (Exception dlqFailure) {
            failure.addSuppressed(dlqFailure);
            throw new IllegalStateException("Kafka DLQ publication was not acknowledged; source offset remains uncommitted", failure);
        }
    }

    private static final class DeliveryFailure extends RuntimeException {
        private final String diagnostic;
        private final int attempts;

        private DeliveryFailure(String diagnostic, int attempts, Throwable cause) {
            super(cause);
            this.diagnostic = diagnostic;
            this.attempts = attempts;
        }
    }
}
