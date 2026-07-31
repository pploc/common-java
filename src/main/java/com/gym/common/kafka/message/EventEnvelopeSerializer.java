package com.gym.common.kafka.message;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;

public class EventEnvelopeSerializer extends JsonSerializer<EventEnvelope<?>> {
    @Override
    public void serialize(EventEnvelope<?> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("event_type", value.eventType());
        gen.writeStringField("key", value.key());
        gen.writeNumberField("timestamp", value.timestamp());
        gen.writeStringField("trace_id", value.traceId());
        gen.writeStringField("source", value.source());

        gen.writeFieldName("payload");
        if (value.payload() != null) {
            String jsonPayload = JsonFormat.printer()
                .omittingInsignificantWhitespace()
                .print(value.payload());
            gen.writeRawValue(jsonPayload);
        } else {
            gen.writeNull();
        }
        gen.writeEndObject();
    }
}
