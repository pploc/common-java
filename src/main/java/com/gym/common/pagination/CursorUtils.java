package com.gym.common.pagination;

import lombok.experimental.UtilityClass;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.Arrays;

@UtilityClass
public class CursorUtils {
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    public static String encode(String value) {
        if (value == null) return null;
        return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    public static String decode(String cursor) {
        if (cursor == null || cursor.isEmpty()) return null;
        try {
            return new String(DECODER.decode(cursor), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid pagination cursor", e);
        }
    }

    public static String encodeCompound(Object... parts) {
        if (parts == null || parts.length == 0) return null;
        String raw = Arrays.stream(parts)
                .map(p -> p != null ? p.toString() : "")
                .collect(Collectors.joining(","));
        return encode(raw);
    }

    public static String[] decodeCompound(String cursor) {
        String decoded = decode(cursor);
        if (decoded == null) return new String[0];
        return decoded.split(",", -1);
    }
}

