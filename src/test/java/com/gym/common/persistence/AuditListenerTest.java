package com.gym.common.persistence;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class AuditListenerTest {

    static class SampleEntity extends BaseEntity {}

    @Test
    void testPrePersistAndPreUpdate() {
        AuditListener listener = new AuditListener();
        SampleEntity entity = new SampleEntity();

        assertNull(entity.getCreatedAt());
        assertNull(entity.getUpdatedAt());

        listener.setCreatedOn(entity);
        assertNotNull(entity.getCreatedAt());
        assertNotNull(entity.getUpdatedAt());

        Instant firstUpdate = entity.getUpdatedAt();
        listener.setUpdatedOn(entity);
        assertNotNull(entity.getUpdatedAt());
    }
}
