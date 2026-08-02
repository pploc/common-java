package com.gym.common.kafka.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Legacy JSON transport envelope. The default Kafka transport is a concrete
 * Protobuf {@link Message} framed by Confluent Schema Registry.
 */
@JsonSerialize(using = EventEnvelopeSerializer.class)
@JsonDeserialize(using = EventEnvelopeDeserializer.class)
public record EventEnvelope<T extends Message>(
    String eventType,
    String key,
    T payload,
    long timestamp,
    String traceId,
    String source,
    String eventId
) {
    public EventEnvelope(
            String eventType,
            String key,
            T payload,
            long timestamp,
            String traceId,
            String source) {
        this(eventType, key, payload, timestamp, traceId, source, null);
    }
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
    private static final Map<Class<?>, Method> NEW_BUILDER_CACHE = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public static <T extends Message> EventEnvelope<T> fromJson(String json, Class<T> payloadClass) throws Exception {
        JsonNode node = MAPPER.readTree(json);

        String eventType = node.get("event_type").asText();
        String key = node.get("key").asText();
        long timestamp = node.get("timestamp").asLong();
        String traceId = node.get("trace_id").asText();
        String source = node.get("source").asText();
        String eventId = node.hasNonNull("event_id") ? node.get("event_id").asText() : null;

        JsonNode payloadNode = node.get("payload");
        T payload = null;
        if (payloadNode != null && !payloadNode.isNull()) {
            Method newBuilderMethod = NEW_BUILDER_CACHE.computeIfAbsent(
                payloadClass,
                clazz -> {
                    try {
                        return clazz.getMethod("newBuilder");
                    } catch (NoSuchMethodException e) {
                        throw new IllegalArgumentException("Payload class " + clazz.getName() + " missing newBuilder method", e);
                    }
                }
            );
            Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
            JSON_PARSER.merge(payloadNode.toString(), builder);
            payload = (T) builder.build();
        }
        return new EventEnvelope<>(eventType, key, payload, timestamp, traceId, source, eventId);
    }
}

