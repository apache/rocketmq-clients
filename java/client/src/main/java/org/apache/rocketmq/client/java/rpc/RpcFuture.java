/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.java.rpc;

import apache.rocketmq.v2.Code;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;

@SuppressWarnings("NullableProblems")
public class RpcFuture<R, T> implements ListenableFuture<T> {
    private final R request;
    private final Context context;
    private final ListenableFuture<T> responseFuture;

    public RpcFuture(Context context, R request, ListenableFuture<T> responseFuture) {
        this.request = request;
        this.context = context;
        this.responseFuture = normalizeTransportException(context, responseFuture);
    }

    public RpcFuture(Throwable t) {
        this.request = null;
        this.context = null;
        this.responseFuture = Futures.immediateFailedFuture(t);
    }

    public R getRequest() {
        return request;
    }

    public Context getContext() {
        return context;
    }

    private static <T> ListenableFuture<T> normalizeTransportException(Context context,
        ListenableFuture<T> responseFuture) {
        if (null == responseFuture) {
            return null;
        }
        return Futures.catchingAsync(responseFuture, StatusRuntimeException.class, exception -> {
            if (Status.Code.RESOURCE_EXHAUSTED != exception.getStatus().getCode()) {
                return Futures.immediateFailedFuture(exception);
            }
            final String requestId = null == context ? null : context.getRequestId();
            final String description = null == exception.getStatus().getDescription()
                ? exception.getMessage() : exception.getStatus().getDescription();
            final TooManyRequestsException tooManyRequestsException = new TooManyRequestsException(
                Code.TOO_MANY_REQUESTS.getNumber(), requestId, description);
            tooManyRequestsException.initCause(exception);
            return Futures.immediateFailedFuture(tooManyRequestsException);
        }, MoreExecutors.directExecutor());
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        responseFuture.addListener(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return responseFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return responseFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return responseFuture.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return responseFuture.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return responseFuture.get(timeout, unit);
    }
}
