package com.gym.common.correlation;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

public class CorrelationIdFilter extends OncePerRequestFilter {

  public static final String HEADER_NAME = "X-Correlation-Id";
  public static final String MDC_KEY = "correlationId";

  private static final int MAX_LENGTH = 64;
  private static final Pattern SANITIZE_PATTERN = Pattern.compile("[^A-Za-z0-9_.\\-]");

  public static String currentCorrelationId() {
    return MDC.get(MDC_KEY);
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws ServletException, IOException {

    String correlationId = sanitise(request.getHeader(HEADER_NAME));
    MDC.put(MDC_KEY, correlationId);
    response.setHeader(HEADER_NAME, correlationId);
    try {
      filterChain.doFilter(request, response);
    } finally {
      MDC.remove(MDC_KEY);
    }
  }

  private static String sanitise(String inbound) {
    if (inbound == null || inbound.isBlank()) {
      return UUID.randomUUID().toString();
    }
    String cleaned = SANITIZE_PATTERN.matcher(inbound).replaceAll("");
    if (cleaned.isEmpty()) {
      return UUID.randomUUID().toString();
    }
    return cleaned.length() > MAX_LENGTH ? cleaned.substring(0, MAX_LENGTH) : cleaned;
  }
}

