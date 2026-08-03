package com.gym.common.kafka;

import com.google.protobuf.Message;
import com.gym.common.kafka.consumer.ConfluentProtobufRecordDecoder;
import com.gym.common.kafka.consumer.PermanentKafkaException;
import com.gym.common.kafka.consumer.RawKafkaHeader;
import com.gym.common.kafka.consumer.RawKafkaRecord;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ConfluentProtobufRecordDecoderTest {
    @Test
    void givenMalformedConfluentFrame_whenDecoding_thenRejectsBeforeSchemaResolution() {
        try (ConfluentProtobufRecordDecoder decoder = new ConfluentProtobufRecordDecoder(mock(KafkaProtobufDeserializer.class))) {
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(record(new byte[]{0, 0, 0, 0, 1})));
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(record(new byte[]{1, 0, 0, 0, 1, 0})));
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(record(new byte[]{0, 0, 0, 0, 0, 0})));
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(record(new byte[]{0, 0, 0, 0, 1, (byte) 0x80})));
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(record(new byte[]{0, 0, 0, 0, 1, 2, (byte) 0x80})));
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(record(new byte[]{0, 0, 0, 0, 1, (byte) 0x81, 0x01, (byte) 0x80})));
        }
    }

    @Test
    void givenInvalidCanonicalHeaders_whenDecoding_thenRejectsBeforeSchemaResolution() {
        try (ConfluentProtobufRecordDecoder decoder = new ConfluentProtobufRecordDecoder(mock(KafkaProtobufDeserializer.class))) {
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(new RawKafkaRecord(
                    "identity.user.registered.v1", 0, 1L, null, frame(), List.of()
            )));
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(recordWithTraceparent("not-w3c")));
            assertThrows(PermanentKafkaException.class, () -> decoder.decode(recordWithTimestamp("-1")));
        }
    }

    private static RawKafkaRecord record(byte[] value) {
        return new RawKafkaRecord("identity.user.registered.v1", 0, 1L, null, value, headers("1700000000000", "00-00000000000000000000000000000001-0000000000000001-01"));
    }

    private static RawKafkaRecord recordWithTraceparent(String traceparent) {
        return new RawKafkaRecord("identity.user.registered.v1", 0, 1L, null, frame(), headers("1700000000000", traceparent));
    }

    private static RawKafkaRecord recordWithTimestamp(String timestamp) {
        return new RawKafkaRecord("identity.user.registered.v1", 0, 1L, null, frame(), headers(timestamp, "00-00000000000000000000000000000001-0000000000000001-01"));
    }

    private static byte[] frame() {
        return new byte[]{0, 0, 0, 0, 1, 0};
    }

    private static List<RawKafkaHeader> headers(String timestamp, String traceparent) {
        return List.of(
                header(KafkaContract.HEADER_EVENT_TYPE, "events.v1.UserRegisteredEvent"),
                header(KafkaContract.HEADER_SOURCE, "test"),
                header(KafkaContract.HEADER_TIMESTAMP, timestamp),
                header(KafkaContract.HEADER_EVENT_ID, "event-1"),
                header(KafkaContract.HEADER_TRACEPARENT, traceparent)
        );
    }

    private static RawKafkaHeader header(String key, String value) {
        return new RawKafkaHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
