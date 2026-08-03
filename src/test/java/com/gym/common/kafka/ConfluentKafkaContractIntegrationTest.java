package com.gym.common.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.gym.common.kafka.config.KafkaAutoConfig;
import com.gym.common.kafka.config.KafkaEventProperties;
import com.gym.common.kafka.consumer.ConfluentProtobufRecordDecoder;
import com.gym.common.kafka.consumer.RawDeliveryCoordinator;
import com.gym.common.kafka.consumer.RawKafkaHeader;
import com.gym.common.kafka.consumer.RawKafkaRecord;
import com.gym.common.kafka.producer.EventPublisherImpl;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Live proof against a Registry seeded only by the immutable gym-proto fixture generator. */
@Tag("kafka-contract")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfluentKafkaContractIntegrationTest {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(20);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private FixtureDocument fixtures;
    private KafkaAutoConfig kafkaConfig;
    private DefaultKafkaProducerFactory<String, Message> protobufProducerFactory;
    private DefaultKafkaProducerFactory<byte[], byte[]> rawProducerFactory;
    private ConfluentProtobufRecordDecoder decoder;

    @BeforeAll
    void givenContractEnvironment_whenInitializing_thenRequiresSeededExternalEndpoints() throws IOException {
        String brokers = requiredEnvironment("KAFKA_BROKERS");
        String registryUrl = requiredEnvironment("SCHEMA_REGISTRY_URL");
        Path fixturePath = Path.of(requiredEnvironment("GYM_PROTO_FIXTURE_PATH"));
        if (!Files.isRegularFile(fixturePath) || !Files.isReadable(fixturePath)) {
            throw new IllegalStateException("GYM_PROTO_FIXTURE_PATH must identify a readable fixture file");
        }

        fixtures = objectMapper.readValue(fixturePath.toFile(), FixtureDocument.class);
        assertFixtureAuthority(fixtures);

        KafkaEventProperties properties = new KafkaEventProperties();
        properties.setSchemaRegistryUrl(registryUrl);
        properties.setAutoRegisterSchemas(false);
        kafkaConfig = new KafkaAutoConfig(properties);
        ReflectionTestUtils.setField(kafkaConfig, "bootstrapServers", brokers);
        ReflectionTestUtils.setField(kafkaConfig, "defaultGroupId", "common-java-contract-" + UUID.randomUUID());

        protobufProducerFactory = cast(kafkaConfig.producerFactory());
        rawProducerFactory = cast(kafkaConfig.rawKafkaProducerFactory());
        assertEquals(false, protobufProducerFactory.getConfigurationProperties().get("auto.register.schemas"));
        decoder = (ConfluentProtobufRecordDecoder) kafkaConfig.rawKafkaDecoder();
    }

    @AfterAll
    void afterAll() {
        if (decoder != null) {
            decoder.close();
        }
        if (protobufProducerFactory != null) {
            protobufProducerFactory.destroy();
        }
        if (rawProducerFactory != null) {
            rawProducerFactory.destroy();
        }
    }

    @Test
    void givenSeededV110Registry_whenDecodingImmutableFrames_thenResolvesAllFrozenMessages() {
        for (FixtureCase fixture : fixtures.cases()) {
            RawKafkaRecord raw = fixture.rawRecord();

            var decoded = decoder.decode(raw);

            assertEquals(fixture.eventType(), decoded.message().getDescriptorForType().getFullName());
            assertArrayEquals(fixture.payload(), decoded.message().toByteArray());
            assertFrameSegments(fixture);
            assertEquals(fixture.subject(), KafkaContract.subjectFor(fixture.topic()));
            assertRawEquals(raw, decoded.raw());
            assertCanonicalHeaders(fixture.headers(), decoded.raw().headers());
        }
    }

    @Test
    void givenLookupOnlyPublisher_whenPublishingImmutableFixtures_thenConsumesExactFramesAndDecodesMessages() {
        for (FixtureCase fixture : fixtures.cases()) {
            Message message = decoder.decode(fixture.rawRecord()).message();
            String key = fixture.keyUtf8() + "-contract-" + UUID.randomUUID();
            EventPublisherImpl fixturePublisher = new EventPublisherImpl(
                    kafkaConfig.kafkaTemplate(protobufProducerFactory),
                    fixture.headers().get(KafkaContract.HEADER_SOURCE),
                    properties(),
                    Clock.fixed(Instant.ofEpochMilli(Long.parseLong(fixture.headers().get(KafkaContract.HEADER_TIMESTAMP))), ZoneOffset.UTC)
            );

            try (Consumer<byte[], byte[]> consumer = newRawConsumer("publisher")) {
                consumer.subscribe(List.of(fixture.topic()));
                pollUntilAssigned(consumer);

                publishWithFixtureTrace(fixture, fixturePublisher, key, message);
                ConsumerRecord<byte[], byte[]> brokerRecord = pollForKey(consumer, key.getBytes(StandardCharsets.UTF_8));
                RawKafkaRecord raw = RawKafkaRecord.from(brokerRecord);
                var decoded = decoder.decode(raw);

                assertArrayEquals(key.getBytes(StandardCharsets.UTF_8), raw.key());
                assertArrayEquals(fixture.completeFrameBytes(), raw.value());
                assertArrayEquals(fixture.payload(), decoded.message().toByteArray());
                assertEquals(fixture.eventType(), decoded.message().getDescriptorForType().getFullName());
                assertCanonicalHeaders(fixture.headers(), raw.headers());
                assertFalse(raw.headers().stream().anyMatch(header -> header.key().startsWith("x-event-")));
            }
        }
    }

    @Test
    void givenPermanentDecodeFailure_whenCoordinatingDelivery_thenPreservesBytesAndAcknowledgesAfterDlqSend() {
        FixtureCase fixture = fixtures.cases().getFirst();
        byte[] malformedValue = fixture.completeFrameBytes();
        malformedValue[0] = 1;
        String key = "dlq-contract-" + UUID.randomUUID();
        List<RawKafkaHeader> headers = new ArrayList<>(fixture.rawRecord().headers());
        headers.add(new RawKafkaHeader("x-preserved", new byte[]{1, 2, 3}));
        headers.add(new RawKafkaHeader("x-preserved", new byte[]{4, 5}));
        RawKafkaRecord raw = new RawKafkaRecord(
                fixture.topic(), 0, 0L, key.getBytes(StandardCharsets.UTF_8), malformedValue, headers
        );
        AtomicBoolean acknowledged = new AtomicBoolean();
        Acknowledgment acknowledgment = () -> acknowledged.set(true);
        KafkaTemplate<byte[], byte[]> rawTemplate = kafkaConfig.rawKafkaTemplate(rawProducerFactory);
        RawDeliveryCoordinator coordinator = new RawDeliveryCoordinator(
                decoder,
                record -> { throw new AssertionError("permanent decoder failure must not reach handler"); },
                kafkaConfig.rawDlqPublisher(rawTemplate),
                duration -> { throw new AssertionError("permanent record must not retry"); }
        );

        try (Consumer<byte[], byte[]> consumer = newRawConsumer("dlq")) {
            consumer.subscribe(List.of(fixture.topic() + ".DLQ"));
            pollUntilAssigned(consumer);

            coordinator.deliver(raw, acknowledgment);
            ConsumerRecord<byte[], byte[]> dlq = pollForKey(consumer, raw.key());

            assertTrue(acknowledged.get());
            assertArrayEquals(raw.key(), dlq.key());
            assertArrayEquals(raw.value(), dlq.value());
            assertDlqHeaders(raw.headers(), dlq.headers(), fixture.topic());
        }
    }

    private KafkaEventProperties properties() {
        KafkaEventProperties properties = new KafkaEventProperties();
        properties.setSchemaRegistryUrl(requiredEnvironment("SCHEMA_REGISTRY_URL"));
        properties.setAutoRegisterSchemas(false);
        return properties;
    }

    private void publishWithFixtureTrace(FixtureCase fixture, EventPublisherImpl fixturePublisher, String key, Message message) {
        String[] parts = fixture.headers().get(KafkaContract.HEADER_TRACEPARENT).split("-");
        SpanContext spanContext = SpanContext.create(parts[1], parts[2], TraceFlags.getSampled(), TraceState.getDefault());
        try (Scope ignored = Span.wrap(spanContext).storeInContext(Context.current()).makeCurrent()) {
            fixturePublisher.publish(fixture.topic(), key, message, fixture.headers().get(KafkaContract.HEADER_EVENT_ID), Map.of());
        }
    }

    private Consumer<byte[], byte[]> newRawConsumer(String name) {
        return kafkaConfig.rawKafkaConsumerFactory().createConsumer(
                "common-java-contract-" + name + "-" + UUID.randomUUID(), null, null, null
        );
    }

    private static void pollUntilAssigned(Consumer<byte[], byte[]> consumer) {
        long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200));
        }
        assertFalse(consumer.assignment().isEmpty(), "consumer did not receive a partition assignment");
    }

    private static ConsumerRecord<byte[], byte[]> pollForKey(Consumer<byte[], byte[]> consumer, byte[] expectedKey) {
        long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(250))) {
                if (Arrays.equals(expectedKey, record.key())) {
                    return record;
                }
            }
        }
        throw new AssertionError("did not receive the contract record before timeout");
    }

    private static void assertFrameSegments(FixtureCase fixture) {
        byte[] complete = fixture.completeFrameBytes();
        byte[] expectedMagicByte = java.util.HexFormat.of().parseHex(fixture.frame().magicByteHex());
        byte[] expectedSchemaId = java.util.HexFormat.of().parseHex(fixture.frame().schemaIdBigEndianHex());
        byte[] expectedIndexes = java.util.HexFormat.of().parseHex(fixture.frame().messageIndexesHex());
        int frameHeaderLength = expectedMagicByte.length + expectedSchemaId.length + expectedIndexes.length;

        assertArrayEquals(expectedMagicByte, Arrays.copyOfRange(complete, 0, expectedMagicByte.length));
        assertArrayEquals(expectedSchemaId, Arrays.copyOfRange(
                complete, expectedMagicByte.length, expectedMagicByte.length + expectedSchemaId.length
        ));
        assertArrayEquals(expectedIndexes, Arrays.copyOfRange(complete, expectedMagicByte.length + expectedSchemaId.length, frameHeaderLength));
        assertArrayEquals(fixture.payload(), Arrays.copyOfRange(complete, frameHeaderLength, complete.length));
    }

    private static void assertRawEquals(RawKafkaRecord expected, RawKafkaRecord actual) {
        assertEquals(expected.topic(), actual.topic());
        assertArrayEquals(expected.key(), actual.key());
        assertArrayEquals(expected.value(), actual.value());
        assertEquals(expected.headers().size(), actual.headers().size());
        for (int index = 0; index < expected.headers().size(); index++) {
            assertEquals(expected.headers().get(index).key(), actual.headers().get(index).key());
            assertArrayEquals(expected.headers().get(index).value(), actual.headers().get(index).value());
        }
    }

    private static void assertCanonicalHeaders(Map<String, String> expected, List<RawKafkaHeader> headers) {
        Map<String, byte[]> actual = headerValues(headers);
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertArrayEquals(entry.getValue().getBytes(StandardCharsets.UTF_8), actual.get(entry.getKey()));
        }
    }

    private static void assertDlqHeaders(List<RawKafkaHeader> original, org.apache.kafka.common.header.Headers headers, String topic) {
        List<Header> actual = new ArrayList<>();
        headers.forEach(actual::add);
        assertEquals(original.size() + 4, actual.size());
        for (int index = 0; index < original.size(); index++) {
            assertEquals(original.get(index).key(), actual.get(index).key());
            assertArrayEquals(original.get(index).value(), actual.get(index).value());
        }

        Map<String, byte[]> appended = new HashMap<>();
        for (Header header : actual.subList(original.size(), actual.size())) {
            appended.put(header.key(), header.value());
        }
        assertEquals(topic, new String(appended.get(KafkaContract.HEADER_ORIGINAL_TOPIC), StandardCharsets.UTF_8));
        assertNotNull(appended.get(KafkaContract.HEADER_EXCEPTION_MESSAGE));
        assertNotNull(appended.get(KafkaContract.HEADER_FAILED_AT));
        assertEquals("1", new String(appended.get(KafkaContract.HEADER_RETRY_COUNT), StandardCharsets.UTF_8));
        assertEquals(4, appended.size());
    }

    private static Map<String, byte[]> headerValues(List<RawKafkaHeader> headers) {
        Map<String, byte[]> values = new HashMap<>();
        for (RawKafkaHeader header : headers) {
            values.put(header.key(), header.value());
        }
        return values;
    }

    private static void assertFixtureAuthority(FixtureDocument fixtureDocument) {
        assertEquals(1, fixtureDocument.fixtureFormatVersion());
        assertEquals("7.7.1", fixtureDocument.generatedBy().schemaRegistryClient());
        assertTrue(fixtureDocument.environment().requireCleanRegistry());
        assertEquals("TopicNameStrategy", fixtureDocument.environment().subjectNameStrategy());
        assertEquals("BACKWARD", fixtureDocument.environment().compatibility());
        assertEquals(KafkaContract.TOPIC_TYPES.size(), fixtureDocument.cases().size());
        for (FixtureCase fixture : fixtureDocument.cases()) {
            assertEquals(KafkaContract.TOPIC_TYPES.get(fixture.topic()), fixture.eventType());
            assertEquals(KafkaContract.subjectFor(fixture.topic()), fixture.subject());
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Kafka contract tests require KAFKA_BROKERS, SCHEMA_REGISTRY_URL, and GYM_PROTO_FIXTURE_PATH");
        }
        return value.trim();
    }

    @SuppressWarnings("unchecked")
    private static <K, V> DefaultKafkaProducerFactory<K, V> cast(Object factory) {
        return (DefaultKafkaProducerFactory<K, V>) factory;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record FixtureDocument(int fixtureFormatVersion, GeneratedBy generatedBy, Environment environment, List<FixtureCase> cases) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record GeneratedBy(String schemaRegistryClient) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Environment(boolean requireCleanRegistry, String subjectNameStrategy, String compatibility) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record FixtureCase(String topic, String keyUtf8, String subject, String eventType, Map<String, String> headers,
                       String payloadHex, Frame frame) {
        RawKafkaRecord rawRecord() {
            return new RawKafkaRecord(topic, 0, 0L, keyUtf8.getBytes(StandardCharsets.UTF_8), completeFrameBytes(), headerList());
        }

        byte[] payload() {
            return java.util.HexFormat.of().parseHex(payloadHex);
        }

        byte[] completeFrameBytes() {
            return java.util.HexFormat.of().parseHex(frame.completeHex());
        }

        List<RawKafkaHeader> headerList() {
            return headers.entrySet().stream()
                    .map(entry -> new RawKafkaHeader(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8)))
                    .toList();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Frame(String magicByteHex, String schemaIdBigEndianHex, String messageIndexesHex, String completeHex) {
    }
}
