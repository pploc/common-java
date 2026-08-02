package com.gym.common.grpc.security;

import io.grpc.Context;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GrpcSecurityContextTest {

    @Test
    void testUserClaimsRecordAndContext() {
        UserClaims claims = new UserClaims("usr-1", "ADMIN", "gym-100");
        assertEquals("usr-1", claims.userId());
        assertEquals("ADMIN", claims.role());
        assertEquals("gym-100", claims.gymId());

        assertNull(GrpcSecurityContext.getCurrentClaims());
        assertNull(GrpcSecurityContext.getUserId());
        assertNull(GrpcSecurityContext.getRole());
        assertNull(GrpcSecurityContext.getGymId());

        Context newCtx = Context.current().withValue(GrpcSecurityContext.CLAIMS_KEY, claims);
        Context previous = newCtx.attach();
        try {
            assertEquals(claims, GrpcSecurityContext.getCurrentClaims());
            assertEquals("usr-1", GrpcSecurityContext.getUserId());
            assertEquals("ADMIN", GrpcSecurityContext.getRole());
            assertEquals("gym-100", GrpcSecurityContext.getGymId());
        } finally {
            newCtx.detach(previous);
        }
    }
}
