package org.apache.rocketmq.test.server;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

public class ResponseWriter {
    protected static final Object INSTANCE_CREATE_LOCK = new Object();
    protected static volatile ResponseWriter instance;

    public static ResponseWriter getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_CREATE_LOCK) {
                if (instance == null) {
                    instance = new ResponseWriter();
                }
            }
        }
        return instance;
    }

    public <T> void write(StreamObserver<T> observer, final T response) {
        if (writeResponse(observer, response)) {
            observer.onCompleted();
        }
    }

    public <T> boolean writeResponse(StreamObserver<T> observer, final T response) {
        if (null == response) {
            return false;
        }
        if (isCancelled(observer)) {
            return false;
        }
        try {
            observer.onNext(response);
        } catch (StatusRuntimeException statusRuntimeException) {
            if (Status.CANCELLED.equals(statusRuntimeException.getStatus())) {
                return false;
            }
            throw statusRuntimeException;
        }
        return true;
    }

    public <T> boolean isCancelled(StreamObserver<T> observer) {
        if (observer instanceof ServerCallStreamObserver) {
            final ServerCallStreamObserver<T> serverCallStreamObserver = (ServerCallStreamObserver<T>) observer;
            return serverCallStreamObserver.isCancelled();
        }
        return false;
    }
}