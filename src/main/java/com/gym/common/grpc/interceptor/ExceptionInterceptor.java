package com.gym.common.grpc.interceptor;

import com.gym.common.error.DomainException;
import com.gym.common.error.ErrorCategory;
import com.gym.common.error.ErrorCode;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

/** Maps all gRPC failures through the frozen status and redaction contract. */
@Slf4j
public class ExceptionInterceptor implements ServerInterceptor {
    private static final String INTERNAL_DESCRIPTION = "Internal server error";
    private static final Metadata.Key<String> ERROR_CODE_KEY =
            Metadata.Key.of("x-error-code", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        ServerCall<ReqT, RespT> guarded = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                super.close(normalizeStatus(status), ensureTrailers(status, trailers));
            }
        };
        try {
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(guarded, headers)) {
                @Override
                public void onHalfClose() {
                    try {
                        super.onHalfClose();
                    } catch (Throwable throwable) {
                        handleException(throwable, guarded);
                    }
                }

                @Override
                public void onMessage(ReqT message) {
                    try {
                        super.onMessage(message);
                    } catch (Throwable throwable) {
                        handleException(throwable, guarded);
                    }
                }
            };
        } catch (Throwable throwable) {
            handleException(throwable, guarded);
            return new ServerCall.Listener<>() {};
        }
    }

    private <ReqT, RespT> void handleException(Throwable throwable, ServerCall<ReqT, RespT> call) {
        Status status;
        Metadata trailers = new Metadata();
        if (throwable instanceof StatusException statusException) {
            status = statusException.getStatus();
            if (statusException.getTrailers() != null) {
                trailers.merge(statusException.getTrailers());
            }
        } else if (throwable instanceof StatusRuntimeException statusRuntimeException) {
            status = statusRuntimeException.getStatus();
            if (statusRuntimeException.getTrailers() != null) {
                trailers.merge(statusRuntimeException.getTrailers());
            }
        } else if (throwable instanceof DomainException domainException) {
            ErrorCode errorCode = domainException.errorCode();
            trailers.put(ERROR_CODE_KEY, errorCode.code());
            status = Status.fromCode(statusFor(errorCode.category()));
            if (errorCode.category() == ErrorCategory.INTERNAL) {
                status = status.withDescription(INTERNAL_DESCRIPTION);
            } else {
                status = status.withDescription(clientSafeDescription(domainException));
            }
            log.warn("gRPC domain failure: code={}, category={}", errorCode.code(), errorCode.category());
        } else {
            status = Status.INTERNAL.withDescription(INTERNAL_DESCRIPTION);
            trailers.put(ERROR_CODE_KEY, "INTERNAL");
            log.error("Unhandled gRPC failure: type={}", throwable.getClass().getName());
        }
        try {
            call.close(status, trailers);
        } catch (IllegalStateException ignored) {
            log.warn("Could not close an already-closed gRPC call");
        }
    }

    private static Status normalizeStatus(Status status) {
        if (status == null) {
            return Status.INTERNAL.withDescription(INTERNAL_DESCRIPTION);
        }
        if (status.getCode() == Status.Code.INTERNAL
                && (status.getDescription() == null || status.getDescription().isBlank()
                || !INTERNAL_DESCRIPTION.equals(status.getDescription()))) {
            // Redact unexpected internal detail while preserving intentional contract text.
            if (status.getDescription() == null || status.getDescription().isBlank()) {
                return Status.INTERNAL.withDescription(INTERNAL_DESCRIPTION);
            }
            if (!INTERNAL_DESCRIPTION.equals(status.getDescription())) {
                return Status.INTERNAL.withDescription(INTERNAL_DESCRIPTION);
            }
        }
        return status;
    }

    private static Metadata ensureTrailers(Status status, Metadata trailers) {
        Metadata out = trailers == null ? new Metadata() : trailers;
        if (!out.containsKey(ERROR_CODE_KEY) && status != null && status.getCode() == Status.Code.INTERNAL) {
            out.put(ERROR_CODE_KEY, "INTERNAL");
        }
        return out;
    }

    private static Status.Code statusFor(ErrorCategory category) {
        return switch (category) {
            case VALIDATION -> Status.Code.INVALID_ARGUMENT;
            case UNAUTHORIZED -> Status.Code.UNAUTHENTICATED;
            case FORBIDDEN -> Status.Code.PERMISSION_DENIED;
            case NOT_FOUND -> Status.Code.NOT_FOUND;
            case CONFLICT -> Status.Code.ALREADY_EXISTS;
            case UNSUPPORTED -> Status.Code.UNIMPLEMENTED;
            case UNPROCESSABLE -> Status.Code.FAILED_PRECONDITION;
            case RATE_LIMITED -> Status.Code.RESOURCE_EXHAUSTED;
            case UNAVAILABLE -> Status.Code.UNAVAILABLE;
            case INTERNAL -> Status.Code.INTERNAL;
        };
    }

    // DomainException text is not trusted. Services may later introduce an
    // explicit safe-message type; until then stable code is the client contract.
    private static String clientSafeDescription(DomainException exception) {
        return exception.errorCode().code();
    }
}
