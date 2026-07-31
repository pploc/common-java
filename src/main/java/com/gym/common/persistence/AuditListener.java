package com.gym.common.persistence;

import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import java.time.Instant;

public class AuditListener {
    @PrePersist
    public void setCreatedOn(BaseEntity entity) {
        Instant now = Instant.now();
        if (entity.getCreatedAt() == null) {
            entity.setCreatedAt(now);
        }
        entity.setUpdatedAt(now);
    }

    @PreUpdate
    public void setUpdatedOn(BaseEntity entity) {
        entity.setUpdatedAt(Instant.now());
    }
}
