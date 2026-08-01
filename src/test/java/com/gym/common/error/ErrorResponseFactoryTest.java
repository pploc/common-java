package com.gym.common.error;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ErrorResponseFactoryTest {

    private final ErrorResponseFactory factory = new ErrorResponseFactory();

    @Test
    void testStatusForCategories() {
        assertEquals(HttpStatus.BAD_REQUEST, factory.statusFor(CommonErrorCode.VALIDATION_FAILED));
        assertEquals(HttpStatus.UNAUTHORIZED, factory.statusFor(CommonErrorCode.UNAUTHENTICATED));
        assertEquals(HttpStatus.FORBIDDEN, factory.statusFor(CommonErrorCode.ACCESS_DENIED));
        assertEquals(HttpStatus.NOT_FOUND, factory.statusFor(CommonErrorCode.ENDPOINT_NOT_FOUND));
        assertEquals(HttpStatus.CONFLICT, factory.statusFor(CommonErrorCode.DATA_INTEGRITY_VIOLATION));
        assertEquals(HttpStatus.METHOD_NOT_ALLOWED, factory.statusFor(CommonErrorCode.METHOD_NOT_ALLOWED));
        assertEquals(HttpStatus.UNSUPPORTED_MEDIA_TYPE, factory.statusFor(CommonErrorCode.UNSUPPORTED_MEDIA_TYPE));
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, factory.statusFor(CommonErrorCode.RATE_LIMITED));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, factory.statusFor(CommonErrorCode.EVENT_PUBLISH_FAILED));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, factory.statusFor(CommonErrorCode.INTERNAL_ERROR));
        
        ErrorCode unprocessableCode = new ErrorCode() {
            @Override
            public String code() { return "UNPROCESSABLE"; }
            @Override
            public ErrorCategory category() { return ErrorCategory.UNPROCESSABLE; }
        };
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, factory.statusFor(unprocessableCode));
    }

    @Test
    void testCreateErrorResponse() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/v1/test");

        ErrorResponse res1 = factory.create(CommonErrorCode.VALIDATION_FAILED, "Invalid field", request);
        assertEquals(HttpStatus.BAD_REQUEST.value(), res1.status());
        assertEquals("VALIDATION_FAILED", res1.code());
        assertEquals("Invalid field", res1.message());
        assertEquals("/api/v1/test", res1.path());

        List<ValidationError> errors = List.of(new ValidationError("age", "-1", "must be positive"));
        ErrorResponse res2 = factory.create(HttpStatus.BAD_REQUEST, CommonErrorCode.VALIDATION_FAILED, "Validation failed", request, errors);
        assertEquals(1, res2.fieldErrors().size());

        ErrorResponse resNullReq = factory.create(CommonErrorCode.INTERNAL_ERROR, "Error", null);
        assertNull(resNullReq.path());
    }
}
