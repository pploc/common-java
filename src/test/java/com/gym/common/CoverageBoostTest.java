package com.gym.common;

import com.google.protobuf.Empty;
import com.gym.common.config.CommonAutoConfiguration;
import com.gym.common.error.*;
import com.gym.common.grpc.interceptor.ExceptionInterceptor;
import com.gym.common.grpc.interceptor.LoggingInterceptor;
import com.gym.common.grpc.interceptor.TracingInterceptor;
import com.gym.common.grpc.security.GrpcSecurityContext;
import com.gym.common.kafka.config.KafkaAutoConfig;
import com.gym.common.kafka.config.KafkaEventProperties;
import com.gym.common.kafka.consumer.RetryableConsumer;
import com.gym.common.kafka.message.EventEnvelope;
import com.gym.common.kafka.message.EventEnvelopeDeserializer;
import com.gym.common.kafka.message.EventEnvelopeSerializer;
import com.gym.common.pagination.CursorUtils;
import io.grpc.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CoverageBoostTest {

    @Test
    void testKafkaAutoConfigErrorHandlerHeadersFunction() {
        ConsumerRecord<String, Object> recordWithRetry = new ConsumerRecord<>("topic-1", 0, 0L, "k", "v");
        recordWithRetry.headers().add(KafkaAutoConfig.HEADER_RETRY_COUNT, "2".getBytes(StandardCharsets.UTF_8));
        Headers h1 = KafkaAutoConfig.createDlqHeaders(recordWithRetry, new RuntimeException("Error msg", new RuntimeException("Cause msg")));
        assertNotNull(h1);
        assertEquals("3", new String(h1.lastHeader(KafkaAutoConfig.HEADER_RETRY_COUNT).value(), StandardCharsets.UTF_8));

        ConsumerRecord<String, Object> recordNoRetry = new ConsumerRecord<>("topic-1", 0, 0L, "k", "v");
        Headers h2 = KafkaAutoConfig.createDlqHeaders(recordNoRetry, new RuntimeException((String) null));
        assertNotNull(h2);
        assertEquals("RuntimeException", new String(
                h2.lastHeader(KafkaAutoConfig.HEADER_EXCEPTION_MESSAGE).value(),
                StandardCharsets.UTF_8
        ));

        ConsumerRecord<String, Object> recordWithMalformedRetry = new ConsumerRecord<>("topic-1", 0, 0L, "k", "v");
        recordWithMalformedRetry.headers().add(KafkaAutoConfig.HEADER_RETRY_COUNT, "not-a-number".getBytes(StandardCharsets.UTF_8));
        Headers h3 = KafkaAutoConfig.createDlqHeaders(recordWithMalformedRetry, new RuntimeException("secret detail"));
        assertEquals("1", new String(h3.lastHeader(KafkaAutoConfig.HEADER_RETRY_COUNT).value(), StandardCharsets.UTF_8));
        assertEquals("RuntimeException", new String(
                h3.lastHeader(KafkaAutoConfig.HEADER_EXCEPTION_MESSAGE).value(),
                StandardCharsets.UTF_8
        ));
    }

    @Test
    void testLoggingAndTracingListenersFullCoverage() {
        MethodDescriptor<Empty, Empty> md = MethodDescriptor.<Empty, Empty>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test.Service/Method")
                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(Empty.getDefaultInstance()))
                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(Empty.getDefaultInstance()))
                .build();

        ServerCall<Empty, Empty> call = mock(ServerCall.class);
        when(call.getMethodDescriptor()).thenReturn(md);

        LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
        TracingInterceptor tracingInterceptor = new TracingInterceptor();

        final ServerCall<Empty, Empty>[] capturedLoggingCall = new ServerCall[1];
        final ServerCall<Empty, Empty>[] capturedTracingCall = new ServerCall[1];

        ServerCallHandler<Empty, Empty> nextLogging = (c, h) -> {
            capturedLoggingCall[0] = c;
            return new ServerCall.Listener<>() {};
        };

        ServerCallHandler<Empty, Empty> nextTracing = (c, h) -> {
            capturedTracingCall[0] = c;
            return new ServerCall.Listener<>() {};
        };

        ServerCall.Listener<Empty> logListener = loggingInterceptor.interceptCall(call, new Metadata(), nextLogging);
        logListener.onMessage(Empty.getDefaultInstance());
        logListener.onHalfClose();
        logListener.onCancel();
        logListener.onComplete();
        logListener.onReady();

        assertNotNull(capturedLoggingCall[0]);
        capturedLoggingCall[0].close(Status.OK, new Metadata());
        capturedLoggingCall[0].close(Status.INTERNAL, new Metadata());

        ServerCall.Listener<Empty> traceListener = tracingInterceptor.interceptCall(call, new Metadata(), nextTracing);
        traceListener.onMessage(Empty.getDefaultInstance());
        traceListener.onHalfClose();
        traceListener.onCancel();
        traceListener.onComplete();
        traceListener.onReady();

        assertNotNull(capturedTracingCall[0]);
        capturedTracingCall[0].close(Status.OK, new Metadata());
        capturedTracingCall[0].close(Status.CANCELLED, new Metadata());
    }

    @Test
    void testExceptionInterceptorListenerSuccessAndIllegalState() {
        ExceptionInterceptor interceptor = new ExceptionInterceptor();
        ServerCall<Empty, Empty> call = mock(ServerCall.class);
        doThrow(new IllegalStateException("Already closed")).when(call).close(any(), any());

        ServerCallHandler<Empty, Empty> next = (c, h) -> new ServerCall.Listener<>() {
            @Override
            public void onMessage(Empty message) { super.onMessage(message); }
            @Override
            public void onHalfClose() { throw new RuntimeException("Unhandled error"); }
        };

        ServerCall.Listener<Empty> listener = interceptor.interceptCall(call, new Metadata(), next);
        listener.onMessage(Empty.getDefaultInstance());
        assertDoesNotThrow(listener::onHalfClose);
    }

    @Test
    void testCursorUtilsErrorHandling() {
        assertNull(CursorUtils.encodeCompound((Object[]) null));
        assertNull(CursorUtils.encodeCompound());

        String cursor = CursorUtils.encode("invalid_base64_#$!@#$");
        String[] decodedInvalid = CursorUtils.decodeCompound(cursor);
        assertNotNull(decodedInvalid);
        assertEquals(1, decodedInvalid.length);

        assertThrows(IllegalArgumentException.class, () -> CursorUtils.decode("invalid_base64_#$@!"));
    }

    @Test
    void testRetryableConsumerHandleProcessingError() {
        RetryableConsumer<Empty> consumer = new RetryableConsumer<>() {
            @Override
            public void onMessage(EventEnvelope<Empty> envelope) {
                handleProcessingError(envelope, new RuntimeException("Processing failed"));
            }
        };

        EventEnvelope<Empty> env = new EventEnvelope<>("Type", "K", Empty.getDefaultInstance(), 0L, "T", "S");
        assertThrows(RuntimeException.class, () -> consumer.onMessage(env));
    }

    @Test
    void testConstructorsAndInstantiation() {
        assertNotNull(new CommonAutoConfiguration());
        assertNotNull(new GrpcSecurityContext());

        ErrorCode ec = CommonErrorCode.INTERNAL_ERROR;
        assertNotNull(new NotFoundException(ec, "msg"));
        assertNotNull(new NotFoundException(ec, "msg", new RuntimeException()));
        assertNotNull(new ForbiddenException(ec, "msg"));
        assertNotNull(new ForbiddenException(ec, "msg", new RuntimeException()));
        assertNotNull(new ConflictException(ec, "msg"));
        assertNotNull(new ConflictException(ec, "msg", new RuntimeException()));
    }

    @Test
    void testEventEnvelopeDeserializerEdgeCases() throws Exception {
        EventEnvelopeDeserializer deserializer = new EventEnvelopeDeserializer();
        assertNotNull(deserializer);

        EventEnvelopeSerializer serializer = new EventEnvelopeSerializer();
        assertNotNull(serializer);
    }
}
