package com.gym.common.kafka.message;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EventEnvelopeDeserializer extends JsonDeserializer<EventEnvelope<?>> implements ContextualDeserializer {

    private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
    private static final Map<Class<?>, Method> NEW_BUILDER_CACHE = new ConcurrentHashMap<>();

    private final JavaType targetPayloadType;

    public EventEnvelopeDeserializer() {
        this(null);
    }

    public EventEnvelopeDeserializer(JavaType targetPayloadType) {
        this.targetPayloadType = targetPayloadType;
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
        JavaType contextualType = ctxt.getContextualType();
        if (contextualType != null && contextualType.hasGenericTypes()) {
            JavaType payloadType = contextualType.containedType(0);
            return new EventEnvelopeDeserializer(payloadType);
        }
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public EventEnvelope<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);

        String eventType = node.has("event_type") ? node.get("event_type").asText() : null;
        String key = node.has("key") ? node.get("key").asText() : null;
        long timestamp = node.has("timestamp") ? node.get("timestamp").asLong() : 0L;
        String traceId = node.has("trace_id") ? node.get("trace_id").asText() : null;
        String source = node.has("source") ? node.get("source").asText() : null;

        JsonNode payloadNode = node.get("payload");
        Message payload = null;

        if (payloadNode != null && !payloadNode.isNull() && targetPayloadType != null) {
            Class<?> rawClass = targetPayloadType.getRawClass();
            if (Message.class.isAssignableFrom(rawClass)) {
                try {
                    Class<? extends Message> msgClass = (Class<? extends Message>) rawClass;
                    Method newBuilderMethod = NEW_BUILDER_CACHE.computeIfAbsent(
                            msgClass,
                            clazz -> {
                                try {
                                    return clazz.getMethod("newBuilder");
                                } catch (NoSuchMethodException e) {
                                    throw new IllegalArgumentException("Class " + clazz.getName() + " missing newBuilder", e);
                                }
                            }
                    );
                    Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
                    JSON_PARSER.merge(payloadNode.toString(), builder);
                    payload = builder.build();
                } catch (Exception e) {
                    throw new JsonMappingException(p, "Failed to deserialize Protobuf payload", e);
                }
            }
        }

        return new EventEnvelope<>(eventType, key, payload, timestamp, traceId, source);
    }
}
