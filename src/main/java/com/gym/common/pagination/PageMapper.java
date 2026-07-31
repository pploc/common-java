package com.gym.common.pagination;

import com.gym.proto.common.v1.PaginationResponse;

public class PageMapper {
    public static PaginationResponse toProtoResponse(CursorPage<?> page) {
        if (page == null) {
            return PaginationResponse.newBuilder().build();
        }
        return PaginationResponse.newBuilder()
                .setTotalRecords((int) page.totalRecords())
                .setNextCursor(page.nextCursor() != null ? page.nextCursor() : "")
                .setHasMore(page.hasMore())
                .setTotalPages(0)
                .build();
    }
}
