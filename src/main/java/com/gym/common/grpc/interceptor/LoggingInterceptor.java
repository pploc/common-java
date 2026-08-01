package com.gym.common.grpc.interceptor;

import io.grpc.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import java.util.UUID;

@Slf4j
public class LoggingInterceptor implements ServerInterceptor {
    private static final Metadata.Key<String> TRACE_ID_HEADER =
            Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final String TRACE_ID_KEY = "traceId";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String fullMethodName = call.getMethodDescriptor().getFullMethodName();
        long startTime = System.nanoTime();

        String traceIdHeader = headers.get(TRACE_ID_HEADER);
        final String traceId = (traceIdHeader == null || traceIdHeader.isEmpty())
                ? UUID.randomUUID().toString()
                : traceIdHeader;

        MDC.put(TRACE_ID_KEY, traceId);
        try {
            log.info("gRPC Start: method={}", fullMethodName);
        } finally {
            MDC.remove(TRACE_ID_KEY);
        }

        ServerCall<ReqT, RespT> loggingCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                MDC.put(TRACE_ID_KEY, traceId);
                try {
                    long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                    if (status.isOk()) {
                        log.info("gRPC End: method={}, status=OK, duration={}ms", fullMethodName, durationMs);
                    } else {
                        log.warn("gRPC End: method={}, status={}, description={}, duration={}ms",
                                fullMethodName, status.getCode(), status.getDescription(), durationMs);
                    }
                    super.close(status, trailers);
                } finally {
                    MDC.remove(TRACE_ID_KEY);
                }
            }
        };

        ServerCall.Listener<ReqT> delegateListener;
        MDC.put(TRACE_ID_KEY, traceId);
        try {
            delegateListener = next.startCall(loggingCall, headers);
        } finally {
            MDC.remove(TRACE_ID_KEY);
        }

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegateListener) {
            @Override
            public void onMessage(ReqT message) {
                runWithMdc(() -> super.onMessage(message));
            }

            @Override
            public void onHalfClose() {
                runWithMdc(super::onHalfClose);
            }

            @Override
            public void onCancel() {
                runWithMdc(() -> {
                    long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                    log.warn("gRPC Cancel: method={}, duration={}ms", fullMethodName, durationMs);
                    super.onCancel();
                });
            }

            @Override
            public void onComplete() {
                runWithMdc(super::onComplete);
            }

            @Override
            public void onReady() {
                runWithMdc(super::onReady);
            }

            private void runWithMdc(Runnable action) {
                MDC.put(TRACE_ID_KEY, traceId);
                try {
                    action.run();
                } finally {
                    MDC.remove(TRACE_ID_KEY);
                }
            }
        };
    }
}


