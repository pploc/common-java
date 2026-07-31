package com.gym.common.error;

public class ConflictException extends DomainException {
    public ConflictException(String message) {
        super(CommonErrorCode.DATA_INTEGRITY_VIOLATION, message);
    }

    public ConflictException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
