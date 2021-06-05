package org.apache.rocketmq.admin;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.ChannelzService;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AdminServer {
    private final Server server;
    private final static int PORT = 15072;

    public AdminServer() {
        this.server = NettyServerBuilder.forPort(PORT)
                                        .addService(ChannelzService.newInstance(128))
                                        .addService(new AdminService())
                                        .addService(ProtoReflectionService.newInstance())
                                        .build();
    }

    public void start() throws IOException {
        this.server.start();
        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            server.awaitTermination(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
    }

    public void shutdown() {
        if (null != server) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (null != server) {
            server.awaitTermination();
        }
    }
}
