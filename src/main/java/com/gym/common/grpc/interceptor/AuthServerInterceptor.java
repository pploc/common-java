package com.gym.common.grpc.interceptor;

import com.gym.common.grpc.security.GrpcSecurityContext;
import com.gym.common.grpc.security.RequireRole;
import com.gym.common.grpc.security.UserClaims;
import io.grpc.*;
import org.springframework.core.annotation.AnnotationUtils;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class AuthServerInterceptor implements ServerInterceptor {

    private final GrpcMethodRegistry methodRegistry;

    public AuthServerInterceptor(GrpcMethodRegistry methodRegistry) {
        this.methodRegistry = methodRegistry;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String userId = headers.get(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER));
        String role = headers.get(Metadata.Key.of("x-user-role", Metadata.ASCII_STRING_MARSHALLER));
        String gymId = headers.get(Metadata.Key.of("x-gym-id", Metadata.ASCII_STRING_MARSHALLER));

        UserClaims claims = new UserClaims(userId, role, gymId);

        Method method = methodRegistry.getJavaMethod(call.getMethodDescriptor().getFullMethodName());
        RequireRole requireRole = null;
        if (method != null) {
            requireRole = AnnotationUtils.findAnnotation(method, RequireRole.class);
            if (requireRole == null) {
                requireRole = AnnotationUtils.findAnnotation(method.getDeclaringClass(), RequireRole.class);
            }
        }

        if (requireRole != null) {
            if (userId == null || role == null) {
                call.close(Status.UNAUTHENTICATED.withDescription("Missing credentials"), new Metadata());
                return new ServerCall.Listener<ReqT>() {};
            }

            List<String> allowedRoles = Arrays.asList(requireRole.value());
            if (!allowedRoles.contains(role)) {
                call.close(Status.PERMISSION_DENIED.withDescription("Insufficient permissions"), new Metadata());
                return new ServerCall.Listener<ReqT>() {};
            }
        }

        Context newContext = Context.current().withValue(GrpcSecurityContext.CLAIMS_KEY, claims);
        return Contexts.interceptCall(newContext, call, headers, next);
    }
}
