package com.gym.common.grpc.interceptor;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import java.util.UUID;

public class LoggingInterceptor implements ServerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(LoggingInterceptor.class);
    private static final String TRACE_ID_KEY = "traceId";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String fullMethodName = call.getMethodDescriptor().getFullMethodName();
        long startTime = System.nanoTime();

        String traceId = headers.get(Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER));
        if (traceId == null || traceId.isEmpty()) {
            traceId = UUID.randomUUID().toString();
        }
        MDC.put(TRACE_ID_KEY, traceId);

        log.info("gRPC Start: method={}", fullMethodName);

        ServerCall<ReqT, RespT> loggingCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                if (status.isOk()) {
                    log.info("gRPC End: method={}, status=OK, duration={}ms", fullMethodName, durationMs);
                } else {
                    log.warn("gRPC End: method={}, status={}, description={}, duration={}ms",
                            fullMethodName, status.getCode(), status.getDescription(), durationMs);
                }
                super.close(status, trailers);
                MDC.remove(TRACE_ID_KEY);
            }
        };

        try {
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                    next.startCall(loggingCall, headers)) {
                @Override
                public void onCancel() {
                    long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                    log.warn("gRPC Cancel: method={}, duration={}ms", fullMethodName, durationMs);
                    super.onCancel();
                    MDC.remove(TRACE_ID_KEY);
                }
            };
        } finally {
            MDC.remove(TRACE_ID_KEY);
        }
    }
}
