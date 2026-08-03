package com.gym.common.grpc.security;

import io.grpc.ServerCall;

/**
 * Verifies a workload from transport-bound peer identity such as a validated mTLS
 * certificate SAN. Request metadata must never be used as workload proof.
 */
@FunctionalInterface
public interface WorkloadIdentityVerifier {
    boolean isVerified(ServerCall<?, ?> call);
}
