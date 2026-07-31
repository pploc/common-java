package com.gym.common.testutil;

import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GrpcTestHelper {
    private final String serverName;
    private final List<BindableService> services = new ArrayList<>();
    private final List<ServerInterceptor> interceptors = new ArrayList<>();
    private Server server;
    private ManagedChannel channel;

    public GrpcTestHelper(String serverName) {
        this.serverName = serverName;
    }

    public GrpcTestHelper addService(BindableService service) {
        this.services.add(service);
        return this;
    }

    public GrpcTestHelper addInterceptor(ServerInterceptor interceptor) {
        this.interceptors.add(interceptor);
        return this;
    }

    public void start() throws IOException {
        InProcessServerBuilder builder = InProcessServerBuilder.forName(serverName).directExecutor();
        for (BindableService s : services) {
            builder.addService(ServerInterceptors.intercept(s, interceptors));
        }
        server = builder.build().start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public void stop() throws InterruptedException {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
