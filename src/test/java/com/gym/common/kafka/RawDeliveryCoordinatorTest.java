package com.gym.common.kafka;

import com.google.protobuf.Empty;
import com.gym.common.kafka.consumer.DecodedKafkaRecord;
import com.gym.common.kafka.consumer.DeliverySleeper;
import com.gym.common.kafka.consumer.KafkaRecordHandler;
import com.gym.common.kafka.consumer.PermanentKafkaException;
import com.gym.common.kafka.consumer.RawDeliveryCoordinator;
import com.gym.common.kafka.consumer.RawDlqPublisher;
import com.gym.common.kafka.consumer.RawKafkaDecoder;
import com.gym.common.kafka.consumer.RawKafkaHeader;
import com.gym.common.kafka.consumer.RawKafkaRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RawDeliveryCoordinatorTest {
    @Test
    void givenSuccessfulHandler_whenDeliveringRecord_thenAcknowledgesSourceOffset() {
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        RawDeliveryCoordinator coordinator = coordinator(record -> { }, (raw, diagnostic, attempts) -> { }, duration -> { });

        coordinator.deliver(raw(), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    void givenRetryableFailure_whenRetriesExhaust_thenAcknowledgesAfterDlqPublication() {
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        List<Duration> delays = new ArrayList<>();
        AtomicInteger dlqAttempts = new AtomicInteger();
        AtomicInteger deliveryAttempts = new AtomicInteger();
        RawDeliveryCoordinator coordinator = coordinator(
                record -> { throw new IllegalStateException("transient"); },
                (raw, diagnostic, attempts) -> {
                    dlqAttempts.incrementAndGet();
                    deliveryAttempts.set(attempts);
                },
                delays::add
        );

        coordinator.deliver(raw(), acknowledgment);

        assertEquals(3, delays.size());
        assertEquals(List.of(Duration.ofSeconds(2), Duration.ofSeconds(4), Duration.ofSeconds(8)), delays);
        assertEquals(1, dlqAttempts.get());
        assertEquals(4, deliveryAttempts.get());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void givenPermanentFailure_whenDelivering_thenSkipsRetriesAndPreservesRawBytesForDlq() {
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        RawKafkaRecord raw = raw();
        List<RawKafkaRecord> dlqRecords = new ArrayList<>();
        AtomicInteger deliveryAttempts = new AtomicInteger();
        RawDeliveryCoordinator coordinator = coordinator(
                record -> { throw new PermanentKafkaException("bad input"); },
                (record, diagnostic, attempts) -> {
                    dlqRecords.add(record);
                    deliveryAttempts.set(attempts);
                },
                duration -> { throw new AssertionError("permanent records must not sleep"); }
        );

        coordinator.deliver(raw, acknowledgment);

        assertEquals(1, dlqRecords.size());
        assertEquals(1, deliveryAttempts.get());
        assertArrayEquals(raw.key(), dlqRecords.getFirst().key());
        assertArrayEquals(raw.value(), dlqRecords.getFirst().value());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void givenDlqFailure_whenDeliveringPermanentRecord_thenLeavesSourceOffsetUnacknowledged() {
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        RawDeliveryCoordinator coordinator = coordinator(
                record -> { throw new PermanentKafkaException("bad input"); },
                (raw, diagnostic, attempts) -> { throw new IllegalStateException("dlq unavailable"); },
                duration -> { }
        );

        assertThrows(IllegalStateException.class, () -> coordinator.deliver(raw(), acknowledgment));
    }

    @Test
    void givenFactoryDependencies_whenCreatingCoordinator_thenDeliversRecord() {
        RawKafkaDecoder decoder = raw -> new DecodedKafkaRecord(raw, Empty.getDefaultInstance());
        RawDlqPublisher dlqPublisher = (raw, diagnostic, attempts) -> { };
        var factory = new com.gym.common.kafka.consumer.RawDeliveryCoordinatorFactory(decoder, dlqPublisher, duration -> { });

        RawDeliveryCoordinator coordinator = factory.forHandler(record -> { });

        coordinator.deliver(raw(), mock(Acknowledgment.class));
    }

    @Test
    void givenMutableSourceBytes_whenCreatingRawRecord_thenDefensivelyCopiesThem() {
        byte[] key = {1};
        byte[] value = {2};
        byte[] header = {3};
        RawKafkaRecord record = new RawKafkaRecord("topic", 0, 1L, key, value, List.of(new RawKafkaHeader("x", header)));
        key[0] = 9;
        value[0] = 9;
        header[0] = 9;

        assertArrayEquals(new byte[]{1}, record.key());
        assertArrayEquals(new byte[]{2}, record.value());
        assertArrayEquals(new byte[]{3}, record.headers().getFirst().value());
    }

    private static RawDeliveryCoordinator coordinator(
            KafkaRecordHandler handler,
            RawDlqPublisher dlqPublisher,
            DeliverySleeper sleeper
    ) {
        RawKafkaDecoder decoder = raw -> new DecodedKafkaRecord(raw, Empty.getDefaultInstance());
        return new RawDeliveryCoordinator(decoder, handler, dlqPublisher, sleeper);
    }

    private static RawKafkaRecord raw() {
        return new RawKafkaRecord("identity.user.registered.v1", 0, 3L, new byte[]{1}, new byte[]{2},
                List.of(new RawKafkaHeader("x-original", new byte[]{3})));
    }
}
