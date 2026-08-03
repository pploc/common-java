package com.gym.common.kafka.consumer;

/** Indicates that retrying the same raw record cannot make it valid. */
public final class PermanentKafkaException extends RuntimeException {
    public PermanentKafkaException(String message) {
        super(message);
    }

    public PermanentKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
