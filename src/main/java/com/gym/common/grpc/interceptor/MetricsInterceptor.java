package com.gym.common.grpc.interceptor;

import io.grpc.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;

@Component
public class MetricsInterceptor implements ServerInterceptor {
    private final MeterRegistry registry;

    public MetricsInterceptor(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String serviceName = call.getMethodDescriptor().getServiceName();
        String methodName = call.getMethodDescriptor().getBareMethodName();
        long startTime = System.nanoTime();

        ServerCall<ReqT, RespT> metricsCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                long durationNanos = System.nanoTime() - startTime;
                Timer.builder("grpc.server.calls")
                        .tag("service", serviceName != null ? serviceName : "unknown")
                        .tag("method", methodName != null ? methodName : "unknown")
                        .tag("status", status.getCode().name())
                        .register(registry)
                        .record(durationNanos, TimeUnit.NANOSECONDS);

                Counter.builder("grpc.server.completed")
                        .tag("service", serviceName != null ? serviceName : "unknown")
                        .tag("method", methodName != null ? methodName : "unknown")
                        .tag("status", status.getCode().name())
                        .register(registry)
                        .increment();
                super.close(status, trailers);
            }
        };
        return next.startCall(metricsCall, headers);
    }
}
