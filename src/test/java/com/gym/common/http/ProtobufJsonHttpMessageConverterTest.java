package com.gym.common.http;

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtobufJsonHttpMessageConverterTest {

    private final ProtobufJsonHttpMessageConverter converter = new ProtobufJsonHttpMessageConverter();

    @Test
    void givenWellKnownWrapperJson_whenRead_thenBuildsProtobufMessage() throws Exception {
        // Given — StringValue JSON is a bare string, not an object with "value"
        String json = "\"hello\"";
        HttpInputMessage input = new SimpleInputMessage(json);

        // When
        StringValue message = (StringValue) converter.read(StringValue.class, input);

        // Then
        assertEquals("hello", message.getValue());
    }

    @Test
    void givenProtobufMessage_whenWrite_thenEmitsJson() throws Exception {
        // Given
        StringValue message = StringValue.of("world");
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        HttpOutputMessage output = new SimpleOutputMessage(body);

        // When
        converter.write(message, MediaType.APPLICATION_JSON, output);

        // Then
        String json = body.toString(StandardCharsets.UTF_8);
        assertEquals("\"world\"", json);
    }

    @Test
    void givenMessageSubclass_whenSupports_thenTrue() {
        assertTrue(converter.canRead(StringValue.class, MediaType.APPLICATION_JSON));
        assertTrue(converter.canWrite(StringValue.class, MediaType.APPLICATION_JSON));
    }

    private static final class SimpleInputMessage implements HttpInputMessage {
        private final byte[] bytes;

        private SimpleInputMessage(String json) {
            this.bytes = json.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public InputStream getBody() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public HttpHeaders getHeaders() {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            return headers;
        }
    }

    private static final class SimpleOutputMessage implements HttpOutputMessage {
        private final ByteArrayOutputStream body;
        private final HttpHeaders headers = new HttpHeaders();

        private SimpleOutputMessage(ByteArrayOutputStream body) {
            this.body = body;
            headers.setContentType(MediaType.APPLICATION_JSON);
        }

        @Override
        public OutputStream getBody() {
            return body;
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }
    }
}
