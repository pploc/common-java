package com.gym.common.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable raw broker material retained until the source record is committed. */
public record RawKafkaRecord(
        String topic,
        int partition,
        long offset,
        byte[] key,
        byte[] value,
        List<RawKafkaHeader> headers
) {
    public RawKafkaRecord {
        Objects.requireNonNull(topic, "topic");
        key = copy(key);
        value = copy(value);
        headers = List.copyOf(headers == null ? List.of() : headers);
    }

    public static RawKafkaRecord from(ConsumerRecord<byte[], byte[]> record) {
        List<RawKafkaHeader> copiedHeaders = new ArrayList<>();
        for (Header header : record.headers()) {
            copiedHeaders.add(new RawKafkaHeader(header.key(), header.value()));
        }
        return new RawKafkaRecord(
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), copiedHeaders
        );
    }

    @Override
    public byte[] key() {
        return copy(key);
    }

    @Override
    public byte[] value() {
        return copy(value);
    }

    private static byte[] copy(byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }
}
