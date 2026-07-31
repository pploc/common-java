package com.gym.common.grpc.interceptor;

import io.grpc.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

public class TracingInterceptor implements ServerInterceptor {
    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("com.gym.common.grpc");
    private static final TextMapGetter<Metadata> getter = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Metadata carrier) {
            return carrier.keys();
        }

        @Override
        public String get(Metadata carrier, String key) {
            return carrier.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
        }
    };

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Context parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), headers, getter);

        Span span = tracer.spanBuilder(call.getMethodDescriptor().getFullMethodName())
                .setParent(parentContext)
                .startSpan();

        io.grpc.Context newGrpcContext = io.grpc.Context.current()
                .withValue(io.grpc.Context.key("otel-span"), span);

        ServerCall<ReqT, RespT> tracingCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                span.setAttribute("grpc.status_code", status.getCode().value());
                if (!status.isOk()) {
                    span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, status.getDescription());
                } else {
                    span.setStatus(io.opentelemetry.api.trace.StatusCode.OK);
                }
                try (Scope scope = span.makeCurrent()) {
                    super.close(status, trailers);
                } finally {
                    span.end();
                }
            }
        };

        try (Scope scope = span.makeCurrent()) {
            return Contexts.interceptCall(newGrpcContext, tracingCall, headers, next);
        }
    }
}
