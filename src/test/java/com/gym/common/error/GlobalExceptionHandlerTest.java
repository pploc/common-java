package com.gym.common.error;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler exceptionHandler;
    private HttpServletRequest request;

    @BeforeEach
    void setUp() {
        ErrorResponseFactory factory = new ErrorResponseFactory();
        exceptionHandler = new GlobalExceptionHandler(factory);
        request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/test");
        when(request.getMethod()).thenReturn("POST");
    }

    @Test
    void testHandleDomainException() {
        DomainException ex = new NotFoundException("User not found");
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleDomainException(ex, request);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertEquals("ENDPOINT_NOT_FOUND", response.getBody().code());
        assertEquals("User not found", response.getBody().message());
    }

    @Test
    void testHandleMethodArgumentNotValid() {
        BindingResult bindingResult = mock(BindingResult.class);
        FieldError fieldError1 = new FieldError("object", "password", "secret123", false, null, null, "too short");
        FieldError fieldError2 = new FieldError("object", "username", "superlongusername".repeat(10), false, null, null, "invalid");
        FieldError fieldError3 = new FieldError("object", "email", null, false, null, null, null);

        when(bindingResult.getFieldErrors()).thenReturn(List.of(fieldError1, fieldError2, fieldError3));
        MethodArgumentNotValidException ex = new MethodArgumentNotValidException(null, bindingResult);

        ResponseEntity<ErrorResponse> response = exceptionHandler.handleMethodArgumentNotValid(ex, request);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(3, response.getBody().fieldErrors().size());
        assertEquals("[redacted]", response.getBody().fieldErrors().get(1).rejectedValue());
    }

    @Test
    void testHandleConstraintViolation() {
        ConstraintViolation<?> violation = mock(ConstraintViolation.class);
        Path path = mock(Path.class);
        when(path.toString()).thenReturn("createUser.user.token");
        when(violation.getPropertyPath()).thenReturn(path);
        when(violation.getInvalidValue()).thenReturn("token123");
        when(violation.getMessage()).thenReturn("invalid token");

        ConstraintViolationException ex = new ConstraintViolationException(Set.of(violation));
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleConstraintViolation(ex, request);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals("[redacted]", response.getBody().fieldErrors().get(0).rejectedValue());

        ResponseEntity<ErrorResponse> responseNull = exceptionHandler.handleConstraintViolation(new ConstraintViolationException(null), request);
        assertEquals(HttpStatus.BAD_REQUEST, responseNull.getStatusCode());
    }

    @Test
    void testHandleTypeMismatch() {
        MethodArgumentTypeMismatchException ex = new MethodArgumentTypeMismatchException("abc", Integer.class, "age", null, null);
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleTypeMismatch(ex, request);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().message().contains("age"));
    }

    @Test
    void testHandleMissingParameter() {
        MissingServletRequestParameterException ex = new MissingServletRequestParameterException("page", "int");
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleMissingParameter(ex, request);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().message().contains("page"));
    }

    @Test
    void testHandleUnreadableBody() {
        HttpMessageNotReadableException ex = new HttpMessageNotReadableException("Invalid JSON", (org.springframework.http.HttpInputMessage) null);
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleUnreadableBody(ex, request);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void testHandleMethodNotSupported() {
        HttpRequestMethodNotSupportedException ex = new HttpRequestMethodNotSupportedException("DELETE", List.of("GET", "POST"));
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleMethodNotSupported(ex, request);
        assertEquals(HttpStatus.METHOD_NOT_ALLOWED, response.getStatusCode());
    }

    @Test
    void testHandleMediaTypeNotSupported() {
        HttpMediaTypeNotSupportedException ex = new HttpMediaTypeNotSupportedException("text/xml");
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleMediaTypeNotSupported(ex, request);
        assertEquals(HttpStatus.UNSUPPORTED_MEDIA_TYPE, response.getStatusCode());
    }

    @Test
    void testHandleNoHandlerFound() {
        NoHandlerFoundException ex = new NoHandlerFoundException("GET", "/api/missing", null);
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleNoHandlerFound(ex, request);
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void testHandleAuthenticationAndAccessDenied() {
        AuthenticationException authEx = new AuthenticationException("Unauthenticated") {};
        ResponseEntity<ErrorResponse> authResp = exceptionHandler.handleAuthentication(authEx, request);
        assertEquals(HttpStatus.UNAUTHORIZED, authResp.getStatusCode());

        AccessDeniedException accessEx = new AccessDeniedException("Denied");
        ResponseEntity<ErrorResponse> accessResp = exceptionHandler.handleAccessDenied(accessEx, request);
        assertEquals(HttpStatus.FORBIDDEN, accessResp.getStatusCode());
    }

    @Test
    void testHandleDataIntegrityAndUnexpected() {
        DataIntegrityViolationException dive = new DataIntegrityViolationException("Constraint violation");
        ResponseEntity<ErrorResponse> diveResp = exceptionHandler.handleDataIntegrityViolation(dive, request);
        assertEquals(HttpStatus.CONFLICT, diveResp.getStatusCode());

        Exception unex = new RuntimeException("Unexpected error");
        ResponseEntity<ErrorResponse> unexResp = exceptionHandler.handleUnexpectedException(unex, request);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, unexResp.getStatusCode());
    }
}
