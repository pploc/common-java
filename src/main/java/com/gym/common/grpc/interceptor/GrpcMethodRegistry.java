package com.gym.common.grpc.interceptor;

import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class GrpcMethodRegistry implements ApplicationListener<ContextRefreshedEvent> {

    private final Map<String, Method> cache = new ConcurrentHashMap<>();
    private final ApplicationContext applicationContext;

    public GrpcMethodRegistry(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Map<String, BindableService> services = applicationContext.getBeansOfType(BindableService.class);
        for (BindableService service : services.values()) {
            Class<?> implClass = service.getClass();
            if (implClass.getName().contains("$$")) {
                implClass = implClass.getSuperclass();
            }

            for (Method method : implClass.getMethods()) {
                for (MethodDescriptor<?, ?> descriptor : service.bindService().getServiceDescriptor().getMethods()) {
                    String fullMethodName = descriptor.getFullMethodName();
                    String rpcMethodName = fullMethodName.substring(fullMethodName.indexOf('/') + 1);

                    if (method.getName().equalsIgnoreCase(rpcMethodName)) {
                        cache.put(fullMethodName, method);
                    }
                }
            }
        }
    }

    public Method getJavaMethod(String fullMethodName) {
        return cache.get(fullMethodName);
    }
}
