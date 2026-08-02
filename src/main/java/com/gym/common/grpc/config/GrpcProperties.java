package com.gym.common.grpc.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "gym.grpc")
public class GrpcProperties {
    public static final int DEFAULT_PORT = 9090;
    public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(15);

    private int port = DEFAULT_PORT;
    private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
}

