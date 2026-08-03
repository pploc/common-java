package com.gym.common.grpc.security;

/** Trusted end-user claims injected by the authenticated gateway boundary. */
public record UserClaims(String userId, String role, String gymId, String membershipStatus) {
    public static final String NONE = "NONE";
    public static final String ACTIVE = "ACTIVE";
    public static final String PAUSED = "PAUSED";
    public static final String EXPIRED = "EXPIRED";

    /** Compatibility constructor for callers that do not require membership gating. */
    public UserClaims(String userId, String role, String gymId) {
        this(userId, role, gymId, NONE);
    }
}
