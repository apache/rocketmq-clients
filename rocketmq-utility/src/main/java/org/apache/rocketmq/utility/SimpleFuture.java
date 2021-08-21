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

package org.apache.rocketmq.utility;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.Executor;

/**
 * A future which only has two kind of status: in-flight/done.
 *
 * <h3>Purpose</h3>
 *
 * <p> Compared to {@link ListenableFuture}, which could handle the case of throwable. {@link SimpleFuture} only
 * focus on the completion status.
 */
public class SimpleFuture {
    private final SettableFuture<Void> future;

    /**
     * Default constructor, future is in-flight once constructed.
     */
    public SimpleFuture() {
        this.future = SettableFuture.create();
    }

    /**
     * Make future as done. then {@link Runnable} in {@link #addListener(Runnable, Executor)} would be submitted.
     */
    public void markAsDone() {
        future.set(null);
    }

    public void addListener(Runnable listener, Executor executor) {
        future.addListener(listener, executor);
    }
}
