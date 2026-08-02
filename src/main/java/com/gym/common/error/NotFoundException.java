package com.gym.common.error;

public class NotFoundException extends DomainException {
    public NotFoundException(String message) {
        super(CommonErrorCode.ENDPOINT_NOT_FOUND, message);
    }

    public NotFoundException(String message, Throwable cause) {
        super(CommonErrorCode.ENDPOINT_NOT_FOUND, message, cause);
    }

    public NotFoundException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public NotFoundException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
