package com.gym.common.config;

import com.gym.common.correlation.CorrelationIdFilter;
import com.gym.common.error.ErrorResponseFactory;
import com.gym.common.error.GlobalExceptionHandler;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;

@AutoConfiguration
@ConditionalOnClass(HttpServletRequest.class)
public class WebCommonConfig {

    @Bean
    public ErrorResponseFactory errorResponseFactory() {
        return new ErrorResponseFactory();
    }

    @Bean
    public GlobalExceptionHandler globalExceptionHandler(ErrorResponseFactory factory) {
        return new GlobalExceptionHandler(factory);
    }

    @Bean
    public FilterRegistrationBean<CorrelationIdFilter> correlationIdFilter() {
        FilterRegistrationBean<CorrelationIdFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new CorrelationIdFilter());
        registrationBean.addUrlPatterns("/*");
        registrationBean.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registrationBean;
    }
}
