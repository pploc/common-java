package com.gym.common.grpc.interceptor;

import build.buf.protovalidate.ValidationResult;
import build.buf.protovalidate.Validator;
import build.buf.protovalidate.exceptions.ValidationException;
import com.google.protobuf.Message;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;

/** Rejects inbound protobuf requests that violate Protovalidate constraints. */
@Slf4j
public class ValidationInterceptor implements ServerInterceptor {
    static final String VALIDATION_ERROR_CODE = "VALIDATION_FAILED";
    private static final Metadata.Key<String> ERROR_CODE_KEY =
            Metadata.Key.of("x-error-code", Metadata.ASCII_STRING_MARSHALLER);

    private final Validator validator;

    public ValidationInterceptor() {
        this(new Validator());
    }

    public ValidationInterceptor(Validator validator) {
        this.validator = validator == null ? new Validator() : validator;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
            private boolean closed;

            @Override
            public void onMessage(ReqT message) {
                if (closed) {
                    return;
                }
                if (message instanceof Message protobuf) {
                    try {
                        ValidationResult result = validator.validate(protobuf);
                        if (!result.isSuccess()) {
                            reject(call, result.toString());
                            return;
                        }
                    } catch (ValidationException exception) {
                        reject(call, exception.getMessage());
                        return;
                    }
                }
                super.onMessage(message);
            }

            private void reject(ServerCall<ReqT, RespT> serverCall, String detail) {
                closed = true;
                Metadata trailers = new Metadata();
                trailers.put(ERROR_CODE_KEY, VALIDATION_ERROR_CODE);
                log.warn("gRPC request failed Protovalidate constraints");
                try {
                    serverCall.close(
                            Status.INVALID_ARGUMENT.withDescription(
                                    detail == null || detail.isBlank() ? VALIDATION_ERROR_CODE : detail),
                            trailers);
                } catch (IllegalStateException ignored) {
                    log.warn("Could not close an already-closed gRPC call after validation failure");
                }
            }
        };
    }
}
