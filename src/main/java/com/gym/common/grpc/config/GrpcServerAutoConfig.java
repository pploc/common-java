package com.gym.common.grpc.config;

import com.gym.common.grpc.interceptor.*;
import io.grpc.*;
import io.grpc.protobuf.services.ProtoReflectionService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@AutoConfiguration
@ConditionalOnClass(Server.class)
@Import({GrpcMethodRegistry.class, MetricsInterceptor.class})
public class GrpcServerAutoConfig {
    private static final Logger log = LoggerFactory.getLogger(GrpcServerAutoConfig.class);

    private Server server;

    @Value("${grpc.server.port:9090}")
    private int port;

    @Bean
    public AuthServerInterceptor authServerInterceptor(GrpcMethodRegistry registry) {
        return new AuthServerInterceptor(registry);
    }

    @Bean
    public ExceptionInterceptor exceptionInterceptor() {
        return new ExceptionInterceptor();
    }

    @Bean
    public LoggingInterceptor loggingInterceptor() {
        return new LoggingInterceptor();
    }

    @Bean
    public TracingInterceptor tracingInterceptor() {
        return new TracingInterceptor();
    }

    @Bean
    public Server grpcServer(
            ApplicationContext applicationContext,
            AuthServerInterceptor authInterceptor,
            ExceptionInterceptor exceptionInterceptor,
            LoggingInterceptor loggingInterceptor,
            MetricsInterceptor metricsInterceptor,
            TracingInterceptor tracingInterceptor) {

        Map<String, BindableService> serviceBeans = applicationContext.getBeansOfType(BindableService.class);
        List<ServerServiceDefinition> services = new ArrayList<>();

        for (BindableService service : serviceBeans.values()) {
            ServerServiceDefinition intercepted = ServerInterceptors.intercept(
                    service,
                    tracingInterceptor,
                    loggingInterceptor,
                    metricsInterceptor,
                    authInterceptor,
                    exceptionInterceptor
            );
            services.add(intercepted);
            log.info("Registered gRPC service: {}", service.getClass().getSimpleName());
        }

        ServerBuilder<?> builder = ServerBuilder.forPort(port)
                .addService(ProtoReflectionService.newInstance());

        services.forEach(builder::addService);
        this.server = builder.build();
        return this.server;
    }

    @PostConstruct
    public void start() throws IOException {
        if (server != null) {
            server.start();
            log.info("gRPC server started on port {}", port);
            Thread awaitThread = new Thread(() -> {
                try {
                    server.awaitTermination();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("gRPC server execution thread interrupted");
                }
            });
            awaitThread.setDaemon(true);
            awaitThread.start();
        }
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (server != null) {
            log.info("Shutting down gRPC server...");
            server.shutdown().awaitTermination(15, TimeUnit.SECONDS);
            log.info("gRPC server shut down complete");
        }
    }
}
