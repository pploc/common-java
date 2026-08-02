package com.gym.common.grpc.interceptor;

import com.gym.common.grpc.security.RequireRole;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GrpcMethodRegistryTest {

    @RequireRole("ADMIN")
    static class DummyService implements BindableService {
        @Override
        public ServerServiceDefinition bindService() {
            MethodDescriptor<com.google.protobuf.Empty, com.google.protobuf.Empty> md = MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName("dummy.DummyService/GetDummy")
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                    .build();

            ServiceDescriptor sd = ServiceDescriptor.newBuilder("dummy.DummyService")
                    .addMethod(md)
                    .build();

            return ServerServiceDefinition.builder(sd)
                    .addMethod(md, mock(ServerCallHandler.class))
                    .build();
        }

        @RequireRole("MANAGER")
        public void getDummy(com.google.protobuf.Empty request, StreamObserver<com.google.protobuf.Empty> observer) {}

        // Non-handler overload helper method
        public void getDummy() {}
    }

    @Test
    void testOnApplicationEventScansAndCachesMethods() {
        ApplicationContext ctx = mock(ApplicationContext.class);
        DummyService service = new DummyService();
        when(ctx.getBeansOfType(BindableService.class)).thenReturn(Map.of("dummyService", service));

        GrpcMethodRegistry registry = new GrpcMethodRegistry(ctx);
        registry.onApplicationEvent(mock(ContextRefreshedEvent.class));

        Method m = registry.getJavaMethod("dummy.DummyService/GetDummy");
        assertNotNull(m);
        assertEquals("getDummy", m.getName());
        assertEquals(2, m.getParameterCount());
    }
}
