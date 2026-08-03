package com.gym.common.grpc.interceptor;

import com.gym.common.grpc.security.GrpcSecurityContext;
import com.gym.common.grpc.security.RpcPolicyKind;
import com.gym.common.grpc.security.UserClaims;
import com.gym.common.grpc.security.WorkloadIdentityVerifier;
import io.grpc.*;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Validates gateway-injected end-user claims against an explicit RPC policy. */
public class AuthServerInterceptor implements ServerInterceptor {
    private static final Metadata.Key<String> USER_ID_KEY = key("x-user-id");
    private static final Metadata.Key<String> USER_ROLE_KEY = key("x-user-role");
    private static final Metadata.Key<String> GYM_ID_KEY = key("x-gym-id");
    private static final Metadata.Key<String> MEMBERSHIP_KEY = key("x-membership-status");
    private static final Set<String> ROLES = Set.of("CUSTOMER", "TRAINER", "ADMIN", "SUPER_ADMIN");
    private static final Set<String> MEMBERSHIPS = Set.of("NONE", "ACTIVE", "PAUSED", "EXPIRED");

    private final GrpcMethodRegistry methodRegistry;
    private final WorkloadIdentityVerifier workloadIdentityVerifier;

    public AuthServerInterceptor(GrpcMethodRegistry methodRegistry) {
        this(methodRegistry, call -> false);
    }

    public AuthServerInterceptor(GrpcMethodRegistry methodRegistry, WorkloadIdentityVerifier workloadIdentityVerifier) {
        this.methodRegistry = methodRegistry;
        this.workloadIdentityVerifier = workloadIdentityVerifier;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String method = call.getMethodDescriptor().getFullMethodName();
        GrpcMethodRegistry.MethodPolicy policy = methodRegistry.getPolicy(method);
        if (policy == null) {
            return close(call, Status.PERMISSION_DENIED.withDescription("Method is not authorized"));
        }
        if (policy.kind() == RpcPolicyKind.PUBLIC) {
            return next.startCall(call, headers);
        }
        if (policy.kind() == RpcPolicyKind.INTERNAL_WORKLOAD) {
            if (!workloadIdentityVerifier.isVerified(call)) {
                return close(call, Status.PERMISSION_DENIED.withDescription("Workload identity is required"));
            }
            return next.startCall(call, headers);
        }

        UserClaims claims;
        try {
            claims = claims(headers);
        } catch (IllegalArgumentException exception) {
            return close(call, Status.UNAUTHENTICATED.withDescription("Missing or invalid credentials"));
        }
        if (policy.kind() == RpcPolicyKind.ROLE_RESTRICTED && !List.of(policy.roles()).contains(claims.role())) {
            return close(call, Status.PERMISSION_DENIED.withDescription("Insufficient permissions"));
        }
        if (policy.kind() == RpcPolicyKind.ACTIVE_MEMBERSHIP && !UserClaims.ACTIVE.equals(claims.membershipStatus())) {
            return close(call, Status.PERMISSION_DENIED.withDescription("Active membership is required"));
        }
        Context context = Context.current().withValue(GrpcSecurityContext.CLAIMS_KEY, claims);
        return Contexts.interceptCall(context, call, headers, next);
    }

    private static UserClaims claims(Metadata headers) {
        String userId = required(headers, USER_ID_KEY, false);
        String role = required(headers, USER_ROLE_KEY, true);
        String gymId = optional(headers, GYM_ID_KEY, false);
        String membership = optional(headers, MEMBERSHIP_KEY, true);
        if (!ROLES.contains(role)) {
            throw new IllegalArgumentException("unknown role");
        }
        if (membership != null && !MEMBERSHIPS.contains(membership)) {
            throw new IllegalArgumentException("unknown membership");
        }
        return new UserClaims(userId, role, gymId, membership == null ? UserClaims.NONE : membership);
    }

    private static String required(Metadata headers, Metadata.Key<String> key, boolean normalize) {
        String value = optional(headers, key, normalize);
        if (value == null) {
            throw new IllegalArgumentException("missing " + key.name());
        }
        return value;
    }

    private static String optional(Metadata headers, Metadata.Key<String> key, boolean normalize) {
        Iterable<String> values = headers.getAll(key);
        if (values == null) {
            return null;
        }
        String normalized = null;
        boolean found = false;
        for (String value : values) {
            found = true;
            String candidate = value == null ? "" : value.trim();
            if (normalize) {
                candidate = candidate.toUpperCase(Locale.ROOT);
            }
            if (candidate.isEmpty() || normalized != null && !normalized.equals(candidate)) {
                throw new IllegalArgumentException("invalid " + key.name());
            }
            normalized = candidate;
        }
        return found ? normalized : null;
    }

    private static Metadata.Key<String> key(String name) {
        return Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
    }

    private static <ReqT, RespT> ServerCall.Listener<ReqT> close(ServerCall<ReqT, RespT> call, Status status) {
        call.close(status, new Metadata());
        return new ServerCall.Listener<>() {};
    }
}
