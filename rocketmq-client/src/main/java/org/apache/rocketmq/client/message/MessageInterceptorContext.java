package org.apache.rocketmq.client.message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class MessageInterceptorContext {
    public static MessageInterceptorContext EMPTY = new MessageInterceptorContext();

    private MessageHookPoint.PointStatus status = MessageHookPoint.PointStatus.UNSET;
    private int messageBatchSize = 1;
    private int messageIndex = 0;
    private int attemptTimes = 1;
    private long duration = 0;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private Throwable throwable = null;
    private final ConcurrentMap<String, String> metadata;

    private MessageInterceptorContext() {
        this.metadata = new ConcurrentHashMap<String, String>();
    }
}
