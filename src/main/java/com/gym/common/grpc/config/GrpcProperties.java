package com.gym.common.grpc.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "gym.grpc")
public class GrpcProperties {
    private int port = 9090;
    private Duration shutdownTimeout = Duration.ofSeconds(15);
}
