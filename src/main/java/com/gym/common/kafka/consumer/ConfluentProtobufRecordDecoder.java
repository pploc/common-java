package com.gym.common.kafka.consumer;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.gym.common.kafka.KafkaContract;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Validates frozen record metadata and delegates Protobuf schema resolution to
 * the Confluent protobuf decoder. The original raw frame remains in the result.
 */
public final class ConfluentProtobufRecordDecoder implements RawKafkaDecoder, AutoCloseable {
    private static final int MIN_CONFLUENT_FRAME_BYTES = 6;

    private final KafkaProtobufDeserializer<Message> deserializer;

    public ConfluentProtobufRecordDecoder(KafkaProtobufDeserializer<Message> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    }

    @Override
    public DecodedKafkaRecord decode(RawKafkaRecord record) {
        Objects.requireNonNull(record, "record");
        validateFrame(record.value());
        Map<String, byte[]> headers = canonicalHeaders(record.headers());
        String eventType = requiredUtf8(headers, KafkaContract.HEADER_EVENT_TYPE);
        validateRequiredHeaders(headers);

        Message message;
        try {
            message = deserializer.deserialize(record.topic(), toHeaders(record.headers()), record.value());
        } catch (org.apache.kafka.common.errors.RetriableException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new PermanentKafkaException("Kafka record has an unsupported Protobuf frame or schema", exception);
        }
        if (message == null) {
            throw new PermanentKafkaException("Kafka record decoded to no Protobuf message");
        }

        try {
            KafkaContract.requireFrozenPair(record.topic(), message);
            KafkaContract.requireValid(message);
        } catch (IllegalArgumentException exception) {
            throw new PermanentKafkaException(exception.getMessage(), exception);
        }
        if (!eventType.equals(message.getDescriptorForType().getFullName())) {
            throw new PermanentKafkaException("Kafka event-type does not match decoded Protobuf descriptor");
        }
        return new DecodedKafkaRecord(record, message);
    }

    @Override
    public void close() {
        deserializer.close();
    }

    private static void validateFrame(byte[] frame) {
        if (frame == null || frame.length < MIN_CONFLUENT_FRAME_BYTES) {
            throw new PermanentKafkaException("Kafka record has a truncated Confluent Protobuf frame");
        }
        if (frame[0] != 0) {
            throw new PermanentKafkaException("Kafka record has an invalid Confluent frame magic byte");
        }
        int schemaId = ByteBuffer.wrap(frame, 1, Integer.BYTES).getInt();
        if (schemaId <= 0) {
            throw new PermanentKafkaException("Kafka record has an invalid Schema Registry ID");
        }
        validateMessageIndexes(frame);
    }

    private static void validateMessageIndexes(byte[] frame) {
        int cursor = 1 + Integer.BYTES;
        Varint encodedCount = readUnsignedVarint(frame, cursor);
        cursor = encodedCount.nextOffset();
        if (encodedCount.value() == 0) {
            return;
        }
        int count = CodedInputStream.decodeZigZag32(encodedCount.value());
        if (count < 0 || count > 128) {
            throw new PermanentKafkaException("Kafka record has an invalid Confluent Protobuf message index count");
        }
        for (int index = 0; index < count; index++) {
            Varint encodedIndex = readUnsignedVarint(frame, cursor);
            if (CodedInputStream.decodeZigZag32(encodedIndex.value()) < 0) {
                throw new PermanentKafkaException("Kafka record has a negative Confluent Protobuf message index");
            }
            cursor = encodedIndex.nextOffset();
        }
    }

    private static Varint readUnsignedVarint(byte[] bytes, int offset) {
        if (offset >= bytes.length) {
            throw new PermanentKafkaException("Kafka record has truncated Confluent Protobuf message indexes");
        }
        CodedInputStream input = CodedInputStream.newInstance(bytes, offset, bytes.length - offset);
        try {
            return new Varint(input.readRawVarint32(), offset + input.getTotalBytesRead());
        } catch (IOException exception) {
            throw new PermanentKafkaException("Kafka record has malformed Confluent Protobuf message indexes", exception);
        }
    }

    private record Varint(int value, int nextOffset) {
    }

    private static Map<String, byte[]> canonicalHeaders(List<RawKafkaHeader> rawHeaders) {
        Map<String, byte[]> values = new HashMap<>();
        for (RawKafkaHeader header : rawHeaders) {
            String key = header.key().toLowerCase(Locale.ROOT);
            if (!KafkaContract.REQUIRED_HEADERS.contains(key) && !KafkaContract.HEADER_TRACESTATE.equals(key)) {
                continue;
            }
            byte[] prior = values.putIfAbsent(key, header.value());
            if (prior != null && !java.util.Arrays.equals(prior, header.value())) {
                throw new PermanentKafkaException("Kafka record contains conflicting canonical headers");
            }
        }
        return values;
    }

    private static void validateRequiredHeaders(Map<String, byte[]> headers) {
        for (String header : KafkaContract.REQUIRED_HEADERS) {
            requiredUtf8(headers, header);
        }
        String timestamp = requiredUtf8(headers, KafkaContract.HEADER_TIMESTAMP);
        try {
            if (Long.parseLong(timestamp) < 0) {
                throw new NumberFormatException(timestamp);
            }
        } catch (NumberFormatException exception) {
            throw new PermanentKafkaException("Kafka timestamp header must be a decimal Unix epoch millisecond value", exception);
        }
        validateTraceparent(requiredUtf8(headers, KafkaContract.HEADER_TRACEPARENT));
    }

    private static String requiredUtf8(Map<String, byte[]> headers, String key) {
        byte[] bytes = headers.get(key);
        if (bytes == null) {
            throw new PermanentKafkaException("Kafka record is missing a required canonical header");
        }
        String value = new String(bytes, StandardCharsets.UTF_8).trim();
        if (value.isEmpty()) {
            throw new PermanentKafkaException("Kafka record has a blank canonical header");
        }
        return value;
    }

    private static void validateTraceparent(String traceparent) {
        if (!traceparent.matches("^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")) {
            throw new PermanentKafkaException("Kafka traceparent header is not valid W3C trace context");
        }
    }

    private static RecordHeaders toHeaders(List<RawKafkaHeader> headers) {
        RecordHeaders kafkaHeaders = new RecordHeaders();
        for (RawKafkaHeader header : headers) {
            kafkaHeaders.add(header.key(), header.value());
        }
        return kafkaHeaders;
    }
}
