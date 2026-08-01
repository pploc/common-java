package com.gym.common.grpc.interceptor;

import com.gym.common.error.DomainException;
import com.gym.common.error.ErrorCode;
import io.grpc.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExceptionInterceptor implements ServerInterceptor {
    private static final Metadata.Key<String> ERROR_CODE_KEY =
            Metadata.Key.of("x-error-code", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        try {
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                    next.startCall(call, headers)) {
                @Override
                public void onHalfClose() {
                    try {
                        super.onHalfClose();
                    } catch (Throwable t) {
                        handleException(t, call);
                    }
                }

                @Override
                public void onMessage(ReqT message) {
                    try {
                        super.onMessage(message);
                    } catch (Throwable t) {
                        handleException(t, call);
                    }
                }
            };
        } catch (Throwable t) {
            handleException(t, call);
            return new ServerCall.Listener<ReqT>() {};
        }
    }

    private <ReqT, RespT> void handleException(Throwable t, ServerCall<ReqT, RespT> call) {
        Status status;
        Metadata trailers = new Metadata();
        if (t instanceof DomainException de) {
            ErrorCode code = de.errorCode();
            Status.Code grpcCode = switch (code.category()) {
                case VALIDATION -> Status.Code.INVALID_ARGUMENT;
                case UNAUTHORIZED -> Status.Code.UNAUTHENTICATED;
                case FORBIDDEN -> Status.Code.PERMISSION_DENIED;
                case NOT_FOUND -> Status.Code.NOT_FOUND;
                case CONFLICT -> Status.Code.ALREADY_EXISTS;
                case UNPROCESSABLE -> Status.Code.FAILED_PRECONDITION;
                case RATE_LIMITED -> Status.Code.RESOURCE_EXHAUSTED;
                case UNAVAILABLE -> Status.Code.UNAVAILABLE;
                default -> Status.Code.INTERNAL;
            };
            status = Status.fromCode(grpcCode).withDescription(de.getMessage());
            trailers.put(ERROR_CODE_KEY, code.code());
            log.warn("Domain exception in gRPC: code={}, message={}", code.code(), de.getMessage());
        } else {
            status = Status.INTERNAL.withDescription("Internal server error");
            log.error("Unhandled exception in gRPC", t);
        }
        try {
            call.close(status, trailers);
        } catch (IllegalStateException e) {
            log.warn("Could not close call, already closed", e);
        }
    }
}

