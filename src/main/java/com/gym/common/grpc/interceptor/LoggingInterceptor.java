package com.gym.common.grpc.interceptor;

import io.grpc.*;
import io.opentelemetry.api.trace.Span;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/** Logs only bounded operational gRPC fields. */
@Slf4j
public class LoggingInterceptor implements ServerInterceptor {
    private static final Metadata.Key<String> TRACE_ID_HEADER =
            Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final String TRACE_ID_KEY = "traceId";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String method = call.getMethodDescriptor().getFullMethodName();
        long started = System.nanoTime();
        String traceId = Span.current().getSpanContext().isValid()
                ? Span.current().getSpanContext().getTraceId()
                : fallbackCorrelation(headers);
        logWithTrace(traceId, () -> log.info("gRPC start: method={}", method));

        ServerCall<ReqT, RespT> loggingCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                long durationMs = (System.nanoTime() - started) / 1_000_000;
                logWithTrace(traceId, () -> log.info("gRPC end: method={}, status={}, duration={}ms",
                        method, status.getCode(), durationMs));
                super.close(status, trailers);
            }
        };
        ServerCall.Listener<ReqT> delegate = next.startCall(loggingCall, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onMessage(ReqT message) {
                logWithTrace(traceId, () -> super.onMessage(message));
            }

            @Override
            public void onHalfClose() {
                logWithTrace(traceId, super::onHalfClose);
            }

            @Override
            public void onCancel() {
                logWithTrace(traceId, super::onCancel);
            }

            @Override
            public void onComplete() {
                logWithTrace(traceId, super::onComplete);
            }

            @Override
            public void onReady() {
                logWithTrace(traceId, super::onReady);
            }
        };
    }

    private static String fallbackCorrelation(Metadata headers) {
        Iterable<String> values = headers.getAll(TRACE_ID_HEADER);
        if (values == null) {
            return null;
        }
        String candidate = null;
        for (String value : values) {
            String normalized = value == null ? "" : value.trim();
            if (normalized.isEmpty() || candidate != null && !candidate.equals(normalized)) {
                return null;
            }
            candidate = normalized;
        }
        return candidate;
    }

    private static void logWithTrace(String traceId, Runnable action) {
        if (traceId != null) {
            MDC.put(TRACE_ID_KEY, traceId);
        }
        try {
            action.run();
        } finally {
            MDC.remove(TRACE_ID_KEY);
        }
    }
}
