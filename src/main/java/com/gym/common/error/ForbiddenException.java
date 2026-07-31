package com.gym.common.error;

public class ForbiddenException extends DomainException {
    public ForbiddenException(String message) {
        super(CommonErrorCode.ACCESS_DENIED, message);
    }

    public ForbiddenException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
