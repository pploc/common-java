package com.gym.common.pagination;

import com.gym.proto.common.v1.PaginationResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PaginationTest {

    @Test
    void testCursorPageAndPageMapper() {
        CursorPage<String> cursorPage = new CursorPage<>(List.of("item1", "item2"), "cursor-123", true, 100L);
        assertEquals(2, cursorPage.items().size());
        assertEquals("cursor-123", cursorPage.nextCursor());
        assertTrue(cursorPage.hasMore());
        assertEquals(100L, cursorPage.totalRecords());

        PaginationResponse protoResp = PageMapper.toProtoResponse(cursorPage);
        assertEquals("cursor-123", protoResp.getNextCursor());
        assertTrue(protoResp.getHasMore());
        assertEquals(100, protoResp.getTotalRecords());

        assertEquals(PaginationResponse.getDefaultInstance(), PageMapper.toProtoResponse((CursorPage<?>) null));
    }

    @Test
    void testNormalPageAndPageMapper() {
        NormalPage<String> normalPage = new NormalPage<>(List.of("a", "b"), 0, 10, 50L, 5);
        assertEquals(0, normalPage.page());
        assertEquals(10, normalPage.size());
        assertEquals(50L, normalPage.totalRecords());
        assertEquals(5, normalPage.totalPages());

        PaginationResponse protoResp = PageMapper.toProtoResponse(normalPage);
        assertEquals(50, protoResp.getTotalRecords());
        assertEquals(5, protoResp.getTotalPages());
        assertTrue(protoResp.getHasMore());

        assertEquals(PaginationResponse.getDefaultInstance(), PageMapper.toProtoResponse((NormalPage<?>) null));
    }
}
