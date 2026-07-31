package com.gym.common.error;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.time.OffsetDateTime;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "timestamp",
  "status",
  "error",
  "code",
  "message",
  "path",
  "traceId",
  "fieldErrors"
})
public record ErrorResponse(
    OffsetDateTime timestamp,
    int status,
    String error,
    String code,
    String message,
    String path,
    String traceId,
    List<ValidationError> fieldErrors) {}
