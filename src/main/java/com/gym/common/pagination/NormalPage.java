package com.gym.common.pagination;

import java.util.List;
import java.util.function.Function;

public record NormalPage<T>(
    List<T> items,
    int page,
    int size,
    long totalRecords,
    int totalPages
) {
    public <U> NormalPage<U> map(Function<T, U> converter) {
        List<U> mappedItems = items.stream().map(converter).toList();
        return new NormalPage<>(mappedItems, page, size, totalRecords, totalPages);
    }
}
