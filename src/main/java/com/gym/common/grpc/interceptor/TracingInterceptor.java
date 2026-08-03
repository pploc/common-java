package com.gym.common.grpc.interceptor;

import io.grpc.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TracingInterceptor implements ServerInterceptor {
    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("com.gym.common.grpc");
    private static final Map<String, Metadata.Key<String>> KEY_CACHE = new ConcurrentHashMap<>();

    private static final TextMapGetter<Metadata> getter = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Metadata carrier) {
            return carrier.keys();
        }

        @Override
        public String get(Metadata carrier, String key) {
            if (key == null) return null;
            Metadata.Key<String> metadataKey = KEY_CACHE.computeIfAbsent(
                    key, k -> Metadata.Key.of(k, Metadata.ASCII_STRING_MARSHALLER));
            return carrier.get(metadataKey);
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
                    span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, status.getCode().name());
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

        ServerCall.Listener<ReqT> delegateListener;
        try (Scope scope = span.makeCurrent()) {
            delegateListener = Contexts.interceptCall(newGrpcContext, tracingCall, headers, next);
        }

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegateListener) {
            @Override
            public void onMessage(ReqT message) {
                try (Scope scope = span.makeCurrent()) {
                    super.onMessage(message);
                }
            }

            @Override
            public void onHalfClose() {
                try (Scope scope = span.makeCurrent()) {
                    super.onHalfClose();
                }
            }

            @Override
            public void onCancel() {
                try (Scope scope = span.makeCurrent()) {
                    super.onCancel();
                }
            }

            @Override
            public void onComplete() {
                try (Scope scope = span.makeCurrent()) {
                    super.onComplete();
                }
            }

            @Override
            public void onReady() {
                try (Scope scope = span.makeCurrent()) {
                    super.onReady();
                }
            }
        };
    }
}


