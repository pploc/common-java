package com.gym.common.grpc.interceptor;

import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import lombok.RequiredArgsConstructor;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class GrpcMethodRegistry implements ApplicationListener<ContextRefreshedEvent> {

    private final Map<String, Method> cache = new ConcurrentHashMap<>();
    private final ApplicationContext applicationContext;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Map<String, BindableService> services = applicationContext.getBeansOfType(BindableService.class);
        for (BindableService service : services.values()) {
            Class<?> implClass = AopUtils.getTargetClass(service);

            Map<String, String> rpcNameToFullName = new HashMap<>();
            for (MethodDescriptor<?, ?> descriptor : service.bindService().getServiceDescriptor().getMethods()) {
                String fullMethodName = descriptor.getFullMethodName();
                int slashIdx = fullMethodName.indexOf('/');
                String rpcMethodName = slashIdx >= 0 ? fullMethodName.substring(slashIdx + 1) : fullMethodName;
                rpcNameToFullName.put(rpcMethodName.toLowerCase(), fullMethodName);
            }

            for (Method method : implClass.getMethods()) {
                String fullMethodName = rpcNameToFullName.get(method.getName().toLowerCase());
                if (fullMethodName != null) {
                    cache.put(fullMethodName, method);
                }
            }
        }
    }

    public Method getJavaMethod(String fullMethodName) {
        return cache.get(fullMethodName);
    }
}

