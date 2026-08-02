package com.gym.common.grpc.interceptor;

import io.grpc.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MetricsTracingInterceptorsTest {

    @Test
    void testMetricsInterceptorRecordsCallsAndCompletions() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MetricsInterceptor interceptor = new MetricsInterceptor(registry);

        MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> md = MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test.MetricsService/TestMethod")
                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .build();

        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        when(call.getMethodDescriptor()).thenReturn(md);

        ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> {
            c.close(Status.OK, new Metadata());
            return new ServerCall.Listener<>() {};
        };

        interceptor.interceptCall(call, new Metadata(), next);

        assertNotNull(registry.find("grpc.server.calls").timer());
        assertNotNull(registry.find("grpc.server.completed").counter());
    }

    @Test
    void testTracingInterceptorExecution() {
        TracingInterceptor interceptor = new TracingInterceptor();

        MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> md = MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test.TraceService/TestMethod")
                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .build();

        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        when(call.getMethodDescriptor()).thenReturn(md);

        ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> new ServerCall.Listener<>() {
            @Override
            public void onMessage(com.google.protobuf.Empty message) {}
            @Override
            public void onHalfClose() {}
        };

        ServerCall.Listener<com.google.protobuf.Empty> listener = interceptor.interceptCall(call, new Metadata(), next);
        assertNotNull(listener);
        listener.onMessage(com.google.protobuf.Empty.getDefaultInstance());
        listener.onHalfClose();
    }
}
