package com.gym.common.error;

public class EventPublishFailedException extends DomainException {
    public EventPublishFailedException(String message, Throwable cause) {
        super(CommonErrorCode.EVENT_PUBLISH_FAILED, message, cause);
    }

    public static EventPublishFailedException of(String eventType, Throwable cause) {
        return new EventPublishFailedException("Could not publish event " + eventType + "; transaction rolled back", cause);
    }
}
