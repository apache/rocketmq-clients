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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("NullableProblems")
public class RpcFuture<R, T> implements ListenableFuture<T> {
    private final R request;
    private final Context context;
    private final ListenableFuture<T> responseFuture;

    public RpcFuture(Context context, R request, ListenableFuture<T> responseFuture) {
        this.request = request;
        this.context = context;
        this.responseFuture = responseFuture;
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
