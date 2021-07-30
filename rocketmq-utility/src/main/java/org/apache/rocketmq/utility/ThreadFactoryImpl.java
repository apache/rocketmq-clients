package org.apache.rocketmq.utility;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadFactoryImpl implements ThreadFactory {
    private static final AtomicLong THREAD_INDEX = new AtomicLong(0);
    private static final String THREAD_PREFIX = "Rocketmq";
    private final String threadCustomName;
    private final boolean daemon;

    public ThreadFactoryImpl(final String threadCustomName) {
        this(threadCustomName, false);
    }

    public ThreadFactoryImpl(final String threadCustomName, boolean daemon) {
        this.threadCustomName = threadCustomName;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, THREAD_PREFIX + threadCustomName + "-" + THREAD_INDEX.incrementAndGet());
        thread.setDaemon(daemon);
        return thread;
    }
}
