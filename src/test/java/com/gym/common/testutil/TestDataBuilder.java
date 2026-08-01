package com.gym.common.testutil;

import lombok.experimental.UtilityClass;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

@UtilityClass
public class TestDataBuilder {
    public static <T> T buildTestObject(Supplier<T> supplier, Consumer<T> customizer) {
        T object = supplier.get();
        customizer.accept(object);
        return object;
    }

    public static String randomId() {
        return UUID.randomUUID().toString();
    }

    public static Instant fixedTime() {
        return Instant.parse("2026-07-31T12:00:00Z");
    }
}

