package com.gym.common.correlation;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.MDC;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CorrelationIdFilterTest {

    @Test
    void testCorrelationIdFilterWithInboundHeader() throws Exception {
        CorrelationIdFilter filter = new CorrelationIdFilter();
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain filterChain = mock(FilterChain.class);

        when(request.getHeader(CorrelationIdFilter.HEADER_NAME)).thenReturn("custom-corr-id-123");

        filter.doFilterInternal(request, response, (req, resp) -> {
            assertEquals("custom-corr-id-123", CorrelationIdFilter.currentCorrelationId());
            assertEquals("custom-corr-id-123", MDC.get(CorrelationIdFilter.MDC_KEY));
        });

        verify(response).setHeader(CorrelationIdFilter.HEADER_NAME, "custom-corr-id-123");
        assertNull(CorrelationIdFilter.currentCorrelationId());
    }

    @Test
    void testCorrelationIdFilterWithNullHeaderGeneratesUuid() throws Exception {
        CorrelationIdFilter filter = new CorrelationIdFilter();
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);

        when(request.getHeader(CorrelationIdFilter.HEADER_NAME)).thenReturn(null);

        filter.doFilterInternal(request, response, (req, resp) -> {
            String current = CorrelationIdFilter.currentCorrelationId();
            assertNotNull(current);
            assertFalse(current.isEmpty());
        });

        assertNull(CorrelationIdFilter.currentCorrelationId());
    }

    @Test
    void testSanitiseWithSpecialCharactersAndMaxLength() throws Exception {
        CorrelationIdFilter filter = new CorrelationIdFilter();
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);

        String invalidHeader = "@#$!%^&*()";
        when(request.getHeader(CorrelationIdFilter.HEADER_NAME)).thenReturn(invalidHeader);

        filter.doFilterInternal(request, response, (req, resp) -> {
            String current = CorrelationIdFilter.currentCorrelationId();
            assertNotNull(current);
            assertNotEquals(invalidHeader, current);
        });

        String longHeader = "a".repeat(100);
        when(request.getHeader(CorrelationIdFilter.HEADER_NAME)).thenReturn(longHeader);
        filter.doFilterInternal(request, response, (req, resp) -> {
            String current = CorrelationIdFilter.currentCorrelationId();
            assertEquals(64, current.length());
        });
    }
}
