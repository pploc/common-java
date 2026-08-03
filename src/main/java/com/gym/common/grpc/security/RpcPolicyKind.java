package com.gym.common.grpc.security;

/** Explicit method-level trust classification for every registered gRPC RPC. */
public enum RpcPolicyKind {
    PUBLIC,
    AUTHENTICATED,
    ROLE_RESTRICTED,
    ACTIVE_MEMBERSHIP,
    INTERNAL_WORKLOAD
}
