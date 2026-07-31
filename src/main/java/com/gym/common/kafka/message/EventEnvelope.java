package com.gym.common.kafka.message;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

@JsonSerialize(using = EventEnvelopeSerializer.class)
public record EventEnvelope<T extends Message>(
    String eventType,
    String key,
    T payload,
    long timestamp,
    String traceId,
    String source
) {
    @SuppressWarnings("unchecked")
    public static <T extends Message> EventEnvelope<T> fromJson(String json, Class<T> payloadClass) throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(json);

        String eventType = node.get("event_type").asText();
        String key = node.get("key").asText();
        long timestamp = node.get("timestamp").asLong();
        String traceId = node.get("trace_id").asText();
        String source = node.get("source").asText();

        com.fasterxml.jackson.databind.JsonNode payloadNode = node.get("payload");
        T payload = null;
        if (payloadNode != null && !payloadNode.isNull()) {
            java.lang.reflect.Method newBuilderMethod = payloadClass.getMethod("newBuilder");
            Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
            JsonFormat.parser().ignoringUnknownFields().merge(payloadNode.toString(), builder);
            payload = (T) builder.build();
        }
        return new EventEnvelope<>(eventType, key, payload, timestamp, traceId, source);
    }
}
