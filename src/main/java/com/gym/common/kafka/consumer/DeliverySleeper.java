package com.gym.common.kafka.consumer;

import java.time.Duration;

@FunctionalInterface
public interface DeliverySleeper {
    void sleep(Duration duration) throws InterruptedException;
}
