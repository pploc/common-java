package com.gym.common.config;

import com.gym.common.http.ProtobufJsonHttpMessageConverter;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.HttpMessageConverters;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers {@link ProtobufJsonHttpMessageConverter} for servlet web apps that depend on
 * Spring MVC. Services can return generated protobuf messages from {@code @RestController}
 * methods without handwritten HTTP DTOs.
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass({Message.class, WebMvcConfigurer.class, HttpMessageConverters.ServerBuilder.class})
public class ProtobufJsonWebConfig {

    @Bean
    public WebMvcConfigurer protobufJsonWebMvcConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void configureMessageConverters(HttpMessageConverters.ServerBuilder builder) {
                builder.addCustomConverter(new ProtobufJsonHttpMessageConverter());
            }
        };
    }
}
