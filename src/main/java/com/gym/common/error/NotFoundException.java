package com.gym.common.error;

public class NotFoundException extends DomainException {
    public NotFoundException(String message) {
        super(CommonErrorCode.ENDPOINT_NOT_FOUND, message);
    }

    public NotFoundException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
