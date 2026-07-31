package com.gym.common.pagination;

import java.util.List;

public record NormalPage<T>(
    List<T> items,
    int page,
    int size,
    long totalRecords,
    int totalPages
) {}
