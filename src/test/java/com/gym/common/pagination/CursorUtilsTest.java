package com.gym.common.pagination;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CursorUtilsTest {

    @Test
    void testSimpleEncodeAndDecode() {
        String original = "user_12345";
        String encoded = CursorUtils.encode(original);
        assertNotNull(encoded);
        assertEquals(original, CursorUtils.decode(encoded));
    }

    @Test
    void testNullAndEmptyHandling() {
        assertNull(CursorUtils.encode(null));
        assertNull(CursorUtils.decode(null));
        assertNull(CursorUtils.decode(""));
    }

    @Test
    void testCompoundCursorWithCommas() {
        String part1 = "2026-08-01T12:00:00Z";
        String part2 = "hello,world,test";
        String part3 = "id_999";

        String cursor = CursorUtils.encodeCompound(part1, part2, part3);
        assertNotNull(cursor);

        String[] decoded = CursorUtils.decodeCompound(cursor);
        assertEquals(3, decoded.length);
        assertEquals(part1, decoded[0]);
        assertEquals(part2, decoded[1]);
        assertEquals(part3, decoded[2]);
    }
}
