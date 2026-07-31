package com.gym.common.grpc.security;

public record UserClaims(String userId, String role, String gymId) {}
