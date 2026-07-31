package com.gym.common.error;

import com.gym.common.correlation.CorrelationIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.springframework.http.HttpStatus;

public class ErrorResponseFactory {

  public HttpStatus statusFor(ErrorCode code) {
    return switch (code.category()) {
      case VALIDATION -> HttpStatus.BAD_REQUEST;
      case UNAUTHORIZED -> HttpStatus.UNAUTHORIZED;
      case FORBIDDEN -> HttpStatus.FORBIDDEN;
      case NOT_FOUND -> HttpStatus.NOT_FOUND;
      case CONFLICT -> HttpStatus.CONFLICT;
      case UNSUPPORTED -> unsupportedStatus(code);
      case UNPROCESSABLE -> HttpStatus.UNPROCESSABLE_ENTITY;
      case RATE_LIMITED -> HttpStatus.TOO_MANY_REQUESTS;
      case UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE;
      case INTERNAL -> HttpStatus.INTERNAL_SERVER_ERROR;
    };
  }

  public ErrorResponse create(ErrorCode code, String message, HttpServletRequest request) {
    return create(statusFor(code), code, message, request, null);
  }

  public ErrorResponse create(
      HttpStatus status, ErrorCode code, String message, HttpServletRequest request) {
    return create(status, code, message, request, null);
  }

  public ErrorResponse create(
      HttpStatus status,
      ErrorCode code,
      String message,
      HttpServletRequest request,
      List<ValidationError> fieldErrors) {

    return new ErrorResponse(
        OffsetDateTime.now(ZoneOffset.UTC),
        status.value(),
        status.getReasonPhrase(),
        code.code(),
        message,
        request == null ? null : request.getRequestURI(),
        CorrelationIdFilter.currentCorrelationId(),
        fieldErrors == null || fieldErrors.isEmpty() ? null : fieldErrors);
  }

  private static HttpStatus unsupportedStatus(ErrorCode code) {
    return CommonErrorCode.UNSUPPORTED_MEDIA_TYPE.code().equals(code.code())
        ? HttpStatus.UNSUPPORTED_MEDIA_TYPE
        : HttpStatus.METHOD_NOT_ALLOWED;
  }
}
