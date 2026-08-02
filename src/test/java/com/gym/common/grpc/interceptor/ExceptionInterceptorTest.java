package com.gym.common.grpc.interceptor;

import com.gym.common.error.NotFoundException;
import io.grpc.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExceptionInterceptorTest {

    @Test
    void testExceptionInterceptorCatchesDomainException() {
        ExceptionInterceptor interceptor = new ExceptionInterceptor();
        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        Metadata headers = new Metadata();

        ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> new ServerCall.Listener<>() {
            @Override
            public void onHalfClose() {
                throw new NotFoundException("Resource 123 not found");
            }
        };

        ServerCall.Listener<com.google.protobuf.Empty> listener = interceptor.interceptCall(call, headers, next);
        listener.onHalfClose();

        verify(call).close(argThat(status -> status.getCode() == Status.Code.NOT_FOUND), any());
    }

    @Test
    void testExceptionInterceptorMapsUnsupportedToUnimplemented() {
        ExceptionInterceptor interceptor = new ExceptionInterceptor();
        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> new ServerCall.Listener<>() {
            @Override
            public void onHalfClose() {
                throw new com.gym.common.error.DomainException(
                        com.gym.common.error.CommonErrorCode.METHOD_NOT_ALLOWED,
                        "Method is not supported") {};
            }
        };

        ServerCall.Listener<com.google.protobuf.Empty> listener = interceptor.interceptCall(call, new Metadata(), next);
        listener.onHalfClose();

        verify(call).close(argThat(status -> status.getCode() == Status.Code.UNIMPLEMENTED), any());
    }

    @Test
    void testExceptionInterceptorCatchesDomainExceptionCategories() {
        ExceptionInterceptor interceptor = new ExceptionInterceptor();

        com.gym.common.error.ErrorCode[] codes = new com.gym.common.error.ErrorCode[]{
                com.gym.common.error.CommonErrorCode.VALIDATION_FAILED,
                com.gym.common.error.CommonErrorCode.UNAUTHENTICATED,
                com.gym.common.error.CommonErrorCode.ACCESS_DENIED,
                com.gym.common.error.CommonErrorCode.ENDPOINT_NOT_FOUND,
                com.gym.common.error.CommonErrorCode.DATA_INTEGRITY_VIOLATION,
                com.gym.common.error.CommonErrorCode.RATE_LIMITED,
                com.gym.common.error.CommonErrorCode.EVENT_PUBLISH_FAILED,
                new com.gym.common.error.ErrorCode() {
                    @Override public String code() { return "U"; }
                    @Override public com.gym.common.error.ErrorCategory category() { return com.gym.common.error.ErrorCategory.UNPROCESSABLE; }
                }
        };

        for (com.gym.common.error.ErrorCode code : codes) {
            ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
            ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> new ServerCall.Listener<>() {
                @Override
                public void onHalfClose() {
                    throw new com.gym.common.error.DomainException(code, "Error message") {};
                }
            };

            ServerCall.Listener<com.google.protobuf.Empty> listener = interceptor.interceptCall(call, new Metadata(), next);
            listener.onHalfClose();
            verify(call).close(any(), any());
        }
    }

    @Test
    void testExceptionInterceptorCatchesStartCallException() {
        ExceptionInterceptor interceptor = new ExceptionInterceptor();
        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> {
            throw new RuntimeException("StartCall failed");
        };

        ServerCall.Listener<com.google.protobuf.Empty> listener = interceptor.interceptCall(call, new Metadata(), next);
        assertNotNull(listener);
        verify(call).close(argThat(status -> status.getCode() == Status.Code.INTERNAL), any());
    }

    @Test
    void testExceptionInterceptorCatchesUnhandledException() {
        ExceptionInterceptor interceptor = new ExceptionInterceptor();
        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        Metadata headers = new Metadata();

        ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> new ServerCall.Listener<>() {
            @Override
            public void onMessage(com.google.protobuf.Empty message) {
                throw new NullPointerException("Null reference error");
            }
        };

        ServerCall.Listener<com.google.protobuf.Empty> listener = interceptor.interceptCall(call, headers, next);
        listener.onMessage(com.google.protobuf.Empty.getDefaultInstance());

        verify(call).close(argThat(status -> status.getCode() == Status.Code.INTERNAL), any());
    }
}
