package com.gym.common.kafka.consumer;

import com.gym.common.kafka.KafkaContract;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Acknowledged byte-for-byte DLQ publisher. It never reserializes the source value. */
public final class RawKafkaDlqPublisher implements RawDlqPublisher {
    private static final Set<String> REPLACED_HEADERS = Set.of(
            KafkaContract.HEADER_ORIGINAL_TOPIC,
            KafkaContract.HEADER_EXCEPTION_MESSAGE,
            KafkaContract.HEADER_FAILED_AT,
            KafkaContract.HEADER_RETRY_COUNT
    );

    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final String suffix;
    private final long timeoutMillis;
    private final Clock clock;

    public RawKafkaDlqPublisher(KafkaTemplate<byte[], byte[]> kafkaTemplate, String suffix, long timeoutMillis, Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.suffix = suffix;
        this.timeoutMillis = timeoutMillis;
        this.clock = clock;
    }

    @Override
    public void publish(RawKafkaRecord raw, String diagnostic, int attempts) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(raw.topic() + suffix, raw.key(), raw.value());
        for (RawKafkaHeader header : raw.headers()) {
            if (!REPLACED_HEADERS.contains(header.key().toLowerCase(Locale.ROOT))) {
                record.headers().add(new RecordHeader(header.key(), header.value()));
            }
        }
        add(record, KafkaContract.HEADER_ORIGINAL_TOPIC, raw.topic());
        add(record, KafkaContract.HEADER_EXCEPTION_MESSAGE, diagnostic);
        add(record, KafkaContract.HEADER_FAILED_AT, Long.toString(clock.millis()));
        add(record, KafkaContract.HEADER_RETRY_COUNT, Integer.toString(attempts));

        try {
            kafkaTemplate.send(record).get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw exception;
        }
    }

    private static void add(ProducerRecord<byte[], byte[]> record, String key, String value) {
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }
}
