package com.gym.common.grpc.security;

import io.grpc.Context;

public class GrpcSecurityContext {
    public static final Context.Key<UserClaims> CLAIMS_KEY = Context.key("user-claims");

    public static UserClaims getCurrentClaims() {
        return CLAIMS_KEY.get();
    }

    public static String getUserId() {
        UserClaims claims = getCurrentClaims();
        return claims != null ? claims.userId() : null;
    }

    public static String getRole() {
        UserClaims claims = getCurrentClaims();
        return claims != null ? claims.role() : null;
    }

    public static String getGymId() {
        UserClaims claims = getCurrentClaims();
        return claims != null ? claims.gymId() : null;
    }
}
