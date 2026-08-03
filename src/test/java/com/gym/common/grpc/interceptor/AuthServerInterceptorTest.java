package com.gym.common.grpc.interceptor;

import com.gym.common.grpc.security.GrpcSecurityContext;
import com.gym.common.grpc.security.RequireRole;
import com.gym.common.grpc.security.UserClaims;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AuthServerInterceptorTest {

    @RequireRole("ADMIN")
    static class SecuredService implements BindableService {
        @Override
        public ServerServiceDefinition bindService() {
            MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> md = MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName("secured.SecuredService/AdminOnly")
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                    .build();

            ServiceDescriptor sd = ServiceDescriptor.newBuilder("secured.SecuredService")
                    .addMethod(md)
                    .build();

            return ServerServiceDefinition.builder(sd)
                    .addMethod(md, mock(ServerCallHandler.class))
                    .build();
        }

        @RequireRole({"ADMIN", "MANAGER"})
        public void adminOnly(com.google.protobuf.Empty req, StreamObserver<com.google.protobuf.Empty> obs) {}
    }

    private AuthServerInterceptor interceptor;

    @BeforeEach
    void setUp() {
        ApplicationContext ctx = mock(ApplicationContext.class);
        SecuredService service = new SecuredService();
        when(ctx.getBeansOfType(BindableService.class)).thenReturn(Map.of("service", service));
        GrpcMethodRegistry registry = new GrpcMethodRegistry(ctx);
        registry.onApplicationEvent(mock(ContextRefreshedEvent.class));
        interceptor = new AuthServerInterceptor(registry);
    }

    @Test
    void testInterceptCallWithoutRequireRoleInjectsClaims() {
        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> md = MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("secured.SecuredService/AdminOnly")
                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .build();
        when(call.getMethodDescriptor()).thenReturn(md);

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), "u-10");
        headers.put(Metadata.Key.of("x-user-role", Metadata.ASCII_STRING_MARSHALLER), "ADMIN");

        AtomicBoolean executed = new AtomicBoolean(false);
        ServerCallHandler<com.google.protobuf.Empty, com.google.protobuf.Empty> next = (c, h) -> {
            UserClaims claims = GrpcSecurityContext.getCurrentClaims();
            assertNotNull(claims);
            assertEquals("u-10", claims.userId());
            assertEquals("ADMIN", claims.role());
            executed.set(true);
            return new ServerCall.Listener<>() {};
        };

        interceptor.interceptCall(call, headers, next);
        assertTrue(executed.get());
    }

    @Test
    void testInterceptCallMissingCredentialsClosesCall() {
        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> md = MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("secured.SecuredService/AdminOnly")
                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .build();
        when(call.getMethodDescriptor()).thenReturn(md);

        Metadata headers = new Metadata();
        interceptor.interceptCall(call, headers, (c, h) -> null);

        verify(call).close(argThat(status -> status.getCode() == Status.Code.UNAUTHENTICATED), any());
    }

    @Test
    void testInterceptCallInsufficientRoleClosesCall() {
        ServerCall<com.google.protobuf.Empty, com.google.protobuf.Empty> call = mock(ServerCall.class);
        MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> md = MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("secured.SecuredService/AdminOnly")
                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                .build();
        when(call.getMethodDescriptor()).thenReturn(md);

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), "u-10");
        headers.put(Metadata.Key.of("x-user-role", Metadata.ASCII_STRING_MARSHALLER), "CUSTOMER");

        interceptor.interceptCall(call, headers, (c, h) -> null);
        verify(call).close(argThat(status -> status.getCode() == Status.Code.PERMISSION_DENIED), any());
    }
}
