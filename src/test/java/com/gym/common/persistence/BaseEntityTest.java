package com.gym.common.persistence;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BaseEntityTest {

    static class TestEntity extends BaseEntity {}

    @Test
    void testIdGenerationOnPrePersist() {
        TestEntity entity = new TestEntity();
        assertNull(entity.getId());

        entity.ensureId();
        assertNotNull(entity.getId());
    }

    @Test
    void testEqualsAndHashCode() {
        TestEntity e1 = new TestEntity();
        e1.setId("123");

        TestEntity e2 = new TestEntity();
        e2.setId("123");

        TestEntity e3 = new TestEntity();
        e3.setId("456");

        assertEquals(e1, e2);
        assertNotEquals(e1, e3);
        assertEquals(e1.hashCode(), e2.hashCode());
    }
}
