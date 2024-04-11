package org.apache.rocketmq.test.server;

import apache.rocketmq.v2.MessagingServiceGrpc;

public class MockServer extends MessagingServiceGrpc.MessagingServiceImplBase {
    private Integer port;

    public MockServer() {
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
}
