package com.gym.common.grpc.interceptor;

import com.gym.common.error.DomainException;
import com.gym.common.error.ErrorCategory;
import com.gym.common.error.ErrorCode;
import io.grpc.*;
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
        try {
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
                @Override
                public void onHalfClose() {
                    try {
                        super.onHalfClose();
                    } catch (Throwable throwable) {
                        handleException(throwable, call);
                    }
                }

                @Override
                public void onMessage(ReqT message) {
                    try {
                        super.onMessage(message);
                    } catch (Throwable throwable) {
                        handleException(throwable, call);
                    }
                }
            };
        } catch (Throwable throwable) {
            handleException(throwable, call);
            return new ServerCall.Listener<>() {};
        }
    }

    private <ReqT, RespT> void handleException(Throwable throwable, ServerCall<ReqT, RespT> call) {
        Metadata trailers = new Metadata();
        Status status = Status.INTERNAL.withDescription(INTERNAL_DESCRIPTION);
        String code = "INTERNAL";
        if (throwable instanceof DomainException domainException) {
            ErrorCode errorCode = domainException.errorCode();
            code = errorCode.code();
            status = Status.fromCode(statusFor(errorCode.category()));
            if (errorCode.category() == ErrorCategory.INTERNAL) {
                status = status.withDescription(INTERNAL_DESCRIPTION);
            } else {
                status = status.withDescription(clientSafeDescription(domainException));
            }
            log.warn("gRPC domain failure: code={}, category={}", errorCode.code(), errorCode.category());
        } else {
            log.error("Unhandled gRPC failure: type={}", throwable.getClass().getName());
        }
        trailers.put(ERROR_CODE_KEY, code);
        try {
            call.close(status, trailers);
        } catch (IllegalStateException ignored) {
            log.warn("Could not close an already-closed gRPC call");
        }
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
