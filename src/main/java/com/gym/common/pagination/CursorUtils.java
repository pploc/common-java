package com.gym.common.pagination;

import lombok.experimental.UtilityClass;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;

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
                .map(p -> p != null ? ENCODER.encodeToString(p.toString().getBytes(StandardCharsets.UTF_8)) : "")
                .collect(Collectors.joining(","));
        return encode(raw);
    }

    public static String[] decodeCompound(String cursor) {
        String decoded = decode(cursor);
        if (decoded == null) return new String[0];
        String[] segments = decoded.split(",", -1);
        String[] result = new String[segments.length];
        for (int i = 0; i < segments.length; i++) {
            if (segments[i].isEmpty()) {
                result[i] = "";
            } else {
                try {
                    result[i] = new String(DECODER.decode(segments[i]), StandardCharsets.UTF_8);
                } catch (IllegalArgumentException e) {
                    result[i] = segments[i];
                }
            }
        }
        return result;
    }
}

