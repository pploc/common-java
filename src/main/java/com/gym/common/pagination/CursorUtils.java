package com.gym.common.pagination;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CursorUtils {
    public static String encode(String value) {
        if (value == null) return null;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    public static String decode(String cursor) {
        if (cursor == null || cursor.isEmpty()) return null;
        try {
            return new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid pagination cursor", e);
        }
    }

    public static String encodeCompound(Object... parts) {
        if (parts == null || parts.length == 0) return null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(parts[i] != null ? parts[i].toString() : "");
        }
        return encode(sb.toString());
    }

    public static String[] decodeCompound(String cursor) {
        String decoded = decode(cursor);
        if (decoded == null) return new String[0];
        return decoded.split(",");
    }
}
