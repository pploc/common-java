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
    private static final Map<String, Class<? extends Message>> DYNAMIC_PROTO_CACHE = new ConcurrentHashMap<>();
    
    private static final String[] DEFAULT_PROTO_PACKAGES = {
            "com.gym.proto.events.v1.",
            "com.gym.proto.member.v1.",
            "com.gym.proto.payment.v1.",
            "com.gym.proto.identity.v1."
    };

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
        String eventId = node.hasNonNull("event_id") ? node.get("event_id").asText() : null;

        JsonNode payloadNode = node.get("payload");
        Object payload = null;

        if (payloadNode != null && !payloadNode.isNull()) {
            Class<?> rawClass = targetPayloadType != null ? targetPayloadType.getRawClass() : null;
            if (rawClass == null || rawClass == Object.class) {
                rawClass = resolveProtobufClassDynamic(eventType);
            }

            if (rawClass != null && Message.class.isAssignableFrom(rawClass)) {
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
            } else {
                payload = payloadNode;
            }
        }

        return new EventEnvelope(eventType, key, (Message) payload, timestamp, traceId, source, eventId);
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Message> resolveProtobufClassDynamic(String eventType) {
        if (eventType == null || eventType.isBlank()) {
            return null;
        }
        return DYNAMIC_PROTO_CACHE.computeIfAbsent(eventType, name -> {
            for (String pkg : DEFAULT_PROTO_PACKAGES) {
                try {
                    Class<?> clazz = Class.forName(pkg + name);
                    if (Message.class.isAssignableFrom(clazz)) {
                        return (Class<? extends Message>) clazz;
                    }
                } catch (ClassNotFoundException ignored) {
                }
            }
            return null;
        });
    }
}
