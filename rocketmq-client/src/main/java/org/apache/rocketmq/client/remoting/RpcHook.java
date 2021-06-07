package org.apache.rocketmq.client.remoting;

public interface RpcHook {
    void doBeforeRequest();
}
