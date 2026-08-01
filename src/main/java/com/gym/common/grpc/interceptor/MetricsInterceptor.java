package com.gym.common.grpc.interceptor;

import io.grpc.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class MetricsInterceptor implements ServerInterceptor {
    private final MeterRegistry registry;
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();
    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();

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
                String svc = serviceName != null ? serviceName : "unknown";
                String mtd = methodName != null ? methodName : "unknown";
                String st = status.getCode().name();

                String cacheKey = svc + ":" + mtd + ":" + st;

                timerCache.computeIfAbsent(cacheKey, k ->
                        Timer.builder("grpc.server.calls")
                                .tag("service", svc)
                                .tag("method", mtd)
                                .tag("status", st)
                                .register(registry)
                ).record(durationNanos, TimeUnit.NANOSECONDS);

                counterCache.computeIfAbsent(cacheKey, k ->
                        Counter.builder("grpc.server.completed")
                                .tag("service", svc)
                                .tag("method", mtd)
                                .tag("status", st)
                                .register(registry)
                ).increment();

                super.close(status, trailers);
            }
        };
        return next.startCall(metricsCall, headers);
    }
}

