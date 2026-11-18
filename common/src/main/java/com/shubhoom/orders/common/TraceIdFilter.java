package com.shubhoom.orders.common;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

// Entry point of a trace: reuse the caller's X-Trace-Id or start a new one, and echo it back.
@Component
class TraceIdFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String id = req.getHeader("X-Trace-Id");
        if (id == null || id.isBlank()) {
            id = UUID.randomUUID().toString();
        }
        MDC.put(Trace.KEY, id);
        res.setHeader("X-Trace-Id", id);
        try {
            chain.doFilter(req, res);
        } finally {
            MDC.remove(Trace.KEY);
        }
    }
}
