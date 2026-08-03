package com.gym.common.grpc.interceptor;

import com.gym.common.grpc.security.RequirePolicy;
import com.gym.common.grpc.security.RequireRole;
import com.gym.common.grpc.security.RpcPolicyKind;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import lombok.RequiredArgsConstructor;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Builds an exact full-method policy table from bound services. */
@Component
@RequiredArgsConstructor
public class GrpcMethodRegistry implements ApplicationListener<ContextRefreshedEvent> {

    public record MethodPolicy(RpcPolicyKind kind, String[] roles) {}

    private final Map<String, MethodPolicy> cache = new ConcurrentHashMap<>();
    private final ApplicationContext applicationContext;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Map<String, BindableService> services = applicationContext.getBeansOfType(BindableService.class);
        for (BindableService service : services.values()) {
            Class<?> implClass = AopUtils.getTargetClass(service);
            for (MethodDescriptor<?, ?> descriptor : service.bindService().getServiceDescriptor().getMethods()) {
                String fullMethodName = descriptor.getFullMethodName();
                Method method = findHandlerMethod(implClass, fullMethodName);
                if (method == null) {
                    throw new IllegalStateException("No handler implementation for registered gRPC method " + fullMethodName);
                }
                if (cache.putIfAbsent(fullMethodName, policyFor(method, implClass, fullMethodName)) != null) {
                    throw new IllegalStateException("Duplicate gRPC method policy " + fullMethodName);
                }
            }
        }
    }

    private static Method findHandlerMethod(Class<?> implClass, String fullMethodName) {
        String simpleName = fullMethodName.substring(fullMethodName.lastIndexOf('/') + 1);
        return Arrays.stream(implClass.getMethods())
                .filter(method -> !method.isSynthetic())
                .filter(GrpcMethodRegistry::isGrpcHandlerMethod)
                .filter(method -> method.getName().equalsIgnoreCase(simpleName))
                .reduce((first, second) -> {
                    throw new IllegalStateException("Ambiguous gRPC handler method " + fullMethodName);
                })
                .orElse(null);
    }

    private static MethodPolicy policyFor(Method method, Class<?> implClass, String fullMethodName) {
        RequirePolicy policy = AnnotationUtils.findAnnotation(method, RequirePolicy.class);
        if (policy == null) {
            policy = AnnotationUtils.findAnnotation(implClass, RequirePolicy.class);
        }
        RequireRole role = AnnotationUtils.findAnnotation(method, RequireRole.class);
        if (role == null) {
            role = AnnotationUtils.findAnnotation(implClass, RequireRole.class);
        }
        if (role != null) {
            if (policy != null && policy.value() != RpcPolicyKind.ROLE_RESTRICTED) {
                throw new IllegalStateException("Conflicting role policy for " + fullMethodName);
            }
            return new MethodPolicy(RpcPolicyKind.ROLE_RESTRICTED, role.value());
        }
        if (policy == null) {
            throw new IllegalStateException("Unclassified gRPC method " + fullMethodName);
        }
        return new MethodPolicy(policy.value(), new String[0]);
    }

    private static boolean isGrpcHandlerMethod(Method method) {
        Class<?>[] paramTypes = method.getParameterTypes();
        return paramTypes.length > 0
                && io.grpc.stub.StreamObserver.class.isAssignableFrom(paramTypes[paramTypes.length - 1]);
    }

    public MethodPolicy getPolicy(String fullMethodName) {
        return cache.get(fullMethodName);
    }
}
