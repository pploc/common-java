package com.gym.common.error;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExceptionsAndCodesTest {

    @Test
    void testDomainExceptions() {
        NotFoundException nfe = new NotFoundException("Not found resource");
        assertEquals(CommonErrorCode.ENDPOINT_NOT_FOUND, nfe.errorCode());
        assertEquals("Not found resource", nfe.getMessage());

        NotFoundException nfeCause = new NotFoundException("Not found resource", new RuntimeException("cause"));
        assertNotNull(nfeCause.getCause());

        ForbiddenException fe = new ForbiddenException("Forbidden access");
        assertEquals(CommonErrorCode.ACCESS_DENIED, fe.errorCode());
        assertEquals("Forbidden access", fe.getMessage());

        ForbiddenException feCause = new ForbiddenException("Forbidden access", new RuntimeException("cause"));
        assertNotNull(feCause.getCause());

        ConflictException ce = new ConflictException("Conflict data");
        assertEquals(CommonErrorCode.DATA_INTEGRITY_VIOLATION, ce.errorCode());
        assertEquals("Conflict data", ce.getMessage());

        ConflictException ceCause = new ConflictException("Conflict data", new RuntimeException("cause"));
        assertNotNull(ceCause.getCause());

        EventPublishFailedException epfe = EventPublishFailedException.of("UserCreated", new RuntimeException("Kafka error"));
        assertEquals(CommonErrorCode.EVENT_PUBLISH_FAILED, epfe.errorCode());
        assertTrue(epfe.getMessage().contains("UserCreated"));

        DomainException customDe = new DomainException(CommonErrorCode.INTERNAL_ERROR, "Custom error", new RuntimeException("Cause")) {};
        assertEquals(CommonErrorCode.INTERNAL_ERROR, customDe.errorCode());
        assertEquals("Custom error", customDe.getMessage());
        assertNotNull(customDe.getCause());
    }

    @Test
    void testCommonErrorCodesAndCategory() {
        for (CommonErrorCode code : CommonErrorCode.values()) {
            assertNotNull(code.code());
            assertNotNull(code.category());
            assertEquals(code.name(), code.code());
        }

        assertEquals(ErrorCategory.VALIDATION, CommonErrorCode.VALIDATION_FAILED.category());
        assertEquals(ErrorCategory.UNAUTHORIZED, CommonErrorCode.UNAUTHENTICATED.category());
    }

    @Test
    void testValidationErrorAndResponse() {
        ValidationError ve = new ValidationError("email", "invalid-email", "Email format is invalid");
        assertEquals("email", ve.field());
        assertEquals("invalid-email", ve.rejectedValue());
        assertEquals("Email format is invalid", ve.message());
    }
}
