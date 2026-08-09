package com.gym.common.grpc.interceptor;

import com.gym.proto.events.v1.UserRegisteredEvent;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ValidationInterceptorTest {

    @Test
    void given_invalidProtobufRequest_when_onMessage_then_closesInvalidArgumentWithErrorCode() {
        // given
        ValidationInterceptor interceptor = new ValidationInterceptor();
        ServerCall<UserRegisteredEvent, UserRegisteredEvent> call = mock(ServerCall.class);
        ServerCallHandler<UserRegisteredEvent, UserRegisteredEvent> next =
                (c, h) -> new ServerCall.Listener<>() {};
        UserRegisteredEvent invalid = UserRegisteredEvent.getDefaultInstance();

        // when
        ServerCall.Listener<UserRegisteredEvent> listener =
                interceptor.interceptCall(call, new Metadata(), next);
        listener.onMessage(invalid);

        // then
        verify(call).close(
                argThat(status -> status.getCode() == Status.Code.INVALID_ARGUMENT),
                argThat(trailers -> {
                    Metadata.Key<String> key =
                            Metadata.Key.of("x-error-code", Metadata.ASCII_STRING_MARSHALLER);
                    return ValidationInterceptor.VALIDATION_ERROR_CODE.equals(trailers.get(key));
                }));
    }

    @Test
    void given_validProtobufRequest_when_onMessage_then_doesNotCloseCall() {
        // given
        ValidationInterceptor interceptor = new ValidationInterceptor();
        ServerCall<UserRegisteredEvent, UserRegisteredEvent> call = mock(ServerCall.class);
        ServerCallHandler<UserRegisteredEvent, UserRegisteredEvent> next =
                (c, h) -> new ServerCall.Listener<>() {};
        UserRegisteredEvent valid = UserRegisteredEvent.newBuilder()
                .setUserId("user-001")
                .setEmail("user-001@example.test")
                .setFullName("Fixture User")
                .setRole(com.gym.proto.common.v1.Role.ROLE_CUSTOMER)
                .setAuthProvider(com.gym.proto.common.v1.AuthProvider.AUTH_PROVIDER_LOCAL)
                .setTimestamp(1_700_000_000_123L)
                .build();

        // when
        ServerCall.Listener<UserRegisteredEvent> listener =
                interceptor.interceptCall(call, new Metadata(), next);
        listener.onMessage(valid);

        // then
        verify(call, never()).close(any(), any());
    }
}
