package com.gym.common.grpc.config;

import com.gym.common.grpc.interceptor.*;
import io.grpc.ServerInterceptor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@Slf4j
@AutoConfiguration
@ConditionalOnClass(ServerInterceptor.class)
@Import({GrpcMethodRegistry.class, MetricsInterceptor.class})
@EnableConfigurationProperties(GrpcProperties.class)
@RequiredArgsConstructor
public class GrpcServerAutoConfig {

    private final GrpcProperties grpcProperties;

    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
    public AuthServerInterceptor authServerInterceptor(GrpcMethodRegistry registry, org.springframework.beans.factory.ObjectProvider<com.gym.common.grpc.security.WorkloadIdentityVerifier> verifierProvider) {
        com.gym.common.grpc.security.WorkloadIdentityVerifier verifier = verifierProvider.getIfAvailable(() -> call -> false);
        return new AuthServerInterceptor(registry, verifier);
    }

    @Bean
    public ExceptionInterceptor exceptionInterceptor() {
        return new ExceptionInterceptor();
    }

    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
    public ValidationInterceptor validationInterceptor() {
        return new ValidationInterceptor();
    }

    @Bean
    public LoggingInterceptor loggingInterceptor() {
        return new LoggingInterceptor();
    }

    @Bean
    public TracingInterceptor tracingInterceptor() {
        return new TracingInterceptor();
    }
}


