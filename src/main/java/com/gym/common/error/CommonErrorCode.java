package com.gym.common.error;

public enum CommonErrorCode implements ErrorCode {
    VALIDATION_FAILED(ErrorCategory.VALIDATION),
    MALFORMED_REQUEST(ErrorCategory.VALIDATION),
    ENDPOINT_NOT_FOUND(ErrorCategory.NOT_FOUND),
    METHOD_NOT_ALLOWED(ErrorCategory.UNSUPPORTED),
    UNSUPPORTED_MEDIA_TYPE(ErrorCategory.UNSUPPORTED),
    UNAUTHENTICATED(ErrorCategory.UNAUTHORIZED),
    ACCESS_DENIED(ErrorCategory.FORBIDDEN),
    IDEMPOTENCY_KEY_REUSED(ErrorCategory.CONFLICT),
    IDEMPOTENT_REQUEST_IN_PROGRESS(ErrorCategory.CONFLICT),
    RATE_LIMITED(ErrorCategory.RATE_LIMITED),
    DATA_INTEGRITY_VIOLATION(ErrorCategory.CONFLICT),
    INTERNAL_ERROR(ErrorCategory.INTERNAL),
    EVENT_PUBLISH_FAILED(ErrorCategory.UNAVAILABLE),
    DEPENDENCY_UNAVAILABLE(ErrorCategory.UNAVAILABLE);

    private final ErrorCategory category;

    CommonErrorCode(ErrorCategory category) {
        this.category = category;
    }

    @Override
    public String code() {
        return name();
    }

    @Override
    public ErrorCategory category() {
        return category;
    }
}
