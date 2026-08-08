package com.gym.common.http;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

/**
 * Binds generated protobuf messages as snake_case JSON for Spring MVC.
 * Field names preserve proto names ({@code chain_id}, {@code price_vnd}, …) so HTTP
 * matches the frozen service contracts used by Plans, Member, and future Java edges.
 */
public class ProtobufJsonHttpMessageConverter extends AbstractHttpMessageConverter<Message> {

    private static final JsonFormat.Parser PARSER = JsonFormat.parser().ignoringUnknownFields();
    private static final JsonFormat.Printer PRINTER =
            JsonFormat.printer().preservingProtoFieldNames().omittingInsignificantWhitespace();

    public ProtobufJsonHttpMessageConverter() {
        super(MediaType.APPLICATION_JSON, new MediaType("application", "*+json"));
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        return Message.class.isAssignableFrom(clazz);
    }

    @Override
    protected Message readInternal(Class<? extends Message> clazz, HttpInputMessage inputMessage)
            throws IOException {
        try {
            Method newBuilder = clazz.getMethod("newBuilder");
            Message.Builder builder = (Message.Builder) newBuilder.invoke(null);
            try (Reader reader = new InputStreamReader(inputMessage.getBody(), StandardCharsets.UTF_8)) {
                PARSER.merge(reader, builder);
            }
            return builder.build();
        } catch (IOException ex) {
            throw ex;
        } catch (ReflectiveOperationException | RuntimeException ex) {
            throw new HttpMessageNotReadableException(
                    "Failed to parse protobuf JSON as " + clazz.getSimpleName(), ex, inputMessage);
        }
    }

    @Override
    protected void writeInternal(Message message, HttpOutputMessage outputMessage) throws IOException {
        try (Writer writer = new OutputStreamWriter(outputMessage.getBody(), StandardCharsets.UTF_8)) {
            PRINTER.appendTo(message, writer);
        } catch (IOException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new HttpMessageNotWritableException(
                    "Failed to write protobuf JSON for " + message.getClass().getSimpleName(), ex);
        }
    }
}
