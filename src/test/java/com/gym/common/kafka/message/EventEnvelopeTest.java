package com.gym.common.kafka.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Empty;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EventEnvelopeTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSerializationAndDeserialization() throws Exception {
        Empty payload = Empty.getDefaultInstance();
        EventEnvelope<Empty> envelope = new EventEnvelope<>(
                "EmptyEvent",
                "key-1",
                payload,
                1700000000000L,
                "trace-abc",
                "test-service"
        );

        String json = objectMapper.writeValueAsString(envelope);
        assertNotNull(json);
        assertTrue(json.contains("EmptyEvent"));
        assertTrue(json.contains("trace-abc"));

        EventEnvelope<Empty> fromJsonEnvelope = EventEnvelope.fromJson(json, Empty.class);
        assertEquals("EmptyEvent", fromJsonEnvelope.eventType());
        assertEquals("key-1", fromJsonEnvelope.key());
        assertEquals("trace-abc", fromJsonEnvelope.traceId());
        assertNotNull(fromJsonEnvelope.payload());

        // Test Jackson EventEnvelopeDeserializer
        EventEnvelope<Empty> readValueEnvelope = objectMapper.readValue(json, new com.fasterxml.jackson.core.type.TypeReference<EventEnvelope<Empty>>() {});
        assertNotNull(readValueEnvelope);
        assertEquals("EmptyEvent", readValueEnvelope.eventType());
        assertEquals("key-1", readValueEnvelope.key());

        // Test null/empty payload JSON
        String nullPayloadJson = "{\"event_type\":\"E\",\"key\":\"k\",\"timestamp\":100,\"trace_id\":\"t\",\"source\":\"s\",\"payload\":null}";
        EventEnvelope<Empty> nullPayloadEnv = objectMapper.readValue(nullPayloadJson, new com.fasterxml.jackson.core.type.TypeReference<EventEnvelope<Empty>>() {});
        assertNull(nullPayloadEnv.payload());
    }
}
