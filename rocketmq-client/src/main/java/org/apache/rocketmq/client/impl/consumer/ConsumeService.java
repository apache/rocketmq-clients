package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.listener.MessageListener;

@Slf4j
public abstract class ConsumeService {
    protected final DefaultMQPushConsumerImpl consumerImpl;
    protected final AtomicReference<ServiceState> state;

    @Getter
    private final MessageListener messageListener;

    public ConsumeService(DefaultMQPushConsumerImpl consumerImpl, MessageListener messageListener) {
        this.consumerImpl = consumerImpl;
        this.messageListener = messageListener;
        this.state = new AtomicReference<ServiceState>(ServiceState.READY);
    }

    public void start() {
        synchronized (this) {
            log.info("Begin to start the consume service.");
            if (!state.compareAndSet(ServiceState.READY, ServiceState.STARTED)) {
                log.warn("The consume concurrently service has been started before");
                return;
            }
            log.info("Start the consume service successfully.");
        }
    }

    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the consume service.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("The consume service has not been started before.");
                return;
            }
            state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED);
            log.info("Shutdown the consumer service successfully.");
        }
    }

    public abstract void dispatch();
}
