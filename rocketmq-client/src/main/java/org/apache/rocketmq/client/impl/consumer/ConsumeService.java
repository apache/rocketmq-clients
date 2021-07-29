package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

@Slf4j
public abstract class ConsumeService {
    private static final long CONSUMPTION_DISPATCH_PERIOD_MILLIS = 10;

    protected final DefaultMQPushConsumerImpl consumerImpl;
    protected final AtomicReference<ServiceState> state;
    private final Object dispatcherConditionVariable;
    private final ThreadPoolExecutor dispatcherExecutor;
    @Getter
    private final MessageListener messageListener;

    public ConsumeService(DefaultMQPushConsumerImpl consumerImpl, MessageListener messageListener) {
        this.consumerImpl = consumerImpl;
        this.messageListener = messageListener;
        this.dispatcherConditionVariable = new Object();
        this.dispatcherExecutor = new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("ConsumptionDispatcherThread"));
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
        dispatcherExecutor.submit(new Runnable() {
            @Override
            public void run() {
                while (ServiceState.STARTED == state.get()) {
                    try {
                        dispatch0();
                        synchronized (dispatcherConditionVariable) {
                            dispatcherConditionVariable.wait(CONSUMPTION_DISPATCH_PERIOD_MILLIS);
                        }
                    } catch (Throwable t) {
                        log.error("Exception raised while schedule message consumption dispatcher", t);
                    }
                }
            }
        });
    }

    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the consume service.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("The consume service has not been started before.");
                return;
            }
            this.dispatcherExecutor.shutdown();
            state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED);
            log.info("Shutdown the consumer service successfully.");
        }
    }

    public abstract void dispatch0();

    public ListenableFuture<ConsumeStatus> consume(MessageExt messageExt) {
        final List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(messageExt);
        return consume(messageExtList);
    }

    public ListenableFuture<ConsumeStatus> consume(MessageExt messageExt, long delay, TimeUnit timeUnit) {
        final List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(messageExt);
        return consume(messageExtList, delay, timeUnit);
    }

    public ListenableFuture<ConsumeStatus> consume(List<MessageExt> messageExtList) {
        return consume(messageExtList, 0, TimeUnit.MILLISECONDS);
    }

    public ListenableFuture<ConsumeStatus> consume(List<MessageExt> messageExtList, long delay, TimeUnit timeUnit) {
        final ThreadPoolExecutor consumeExecutor = consumerImpl.getConsumeExecutor();
        final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(consumeExecutor);
        final ConsumeTask task = new ConsumeTask(consumerImpl, messageExtList);
        if (delay <= 0) {
            return executorService.submit(task);
        }
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
        final ListeningScheduledExecutorService schedulerService = MoreExecutors.listeningDecorator(scheduler);
        return schedulerService.schedule(task, delay, timeUnit);
    }

    public void dispatch() {
        synchronized (dispatcherConditionVariable) {
            dispatcherConditionVariable.notify();
        }
    }
}
