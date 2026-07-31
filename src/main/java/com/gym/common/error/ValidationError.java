package com.gym.common.error;

public record ValidationError(String field, String rejectedValue, String message) {}
