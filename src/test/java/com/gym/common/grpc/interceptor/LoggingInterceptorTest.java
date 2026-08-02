package com.gym.common.grpc.interceptor;

import io.grpc.*;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.junit.jupiter.api.Assertions.*;

class LoggingInterceptorTest {

    @Test
    void testMdcTraceIdPropagationDuringListenerCallbacks() {
        LoggingInterceptor interceptor = new LoggingInterceptor();

        MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> methodDescriptor = MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test.TestService/TestMethod")
                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .build();

        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = new NoopServerCall<>() {
            @Override
            public MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> getMethodDescriptor() {
                return methodDescriptor;
            }
        };

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER), "trace-12345");

        final String[] capturedTraceId = new String[1];

        ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> new ServerCall.Listener<>() {
            @Override
            public void onMessage(com.google.protobuf.Empty message) {
                capturedTraceId[0] = MDC.get(LoggingInterceptor.TRACE_ID_KEY);
            }
        };

        ServerCall.Listener<com.google.protobuf.Empty> listener = interceptor.interceptCall(call, headers, next);
        listener.onMessage(com.google.protobuf.Empty.getDefaultInstance());

        assertEquals("trace-12345", capturedTraceId[0]);
    }

    private static class NoopServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {
        @Override
        public void request(int numMessages) {}
        @Override
        public void sendHeaders(Metadata headers) {}
        @Override
        public void sendMessage(RespT message) {}
        @Override
        public void close(Status status, Metadata trailers) {}
        @Override
        public boolean isCancelled() { return false; }
        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() { return null; }
    }
}
