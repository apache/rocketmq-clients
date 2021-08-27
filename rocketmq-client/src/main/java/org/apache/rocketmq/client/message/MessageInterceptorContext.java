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

package org.apache.rocketmq.client.message;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

public class MessageInterceptorContext {
    /**
     * Business status, equal to {@link #rpcStatus} if not set.
     */
    private final MessageHookPointStatus bizStatus;
    /**
     * Rpc status, only make sense for the {@link MessageHookPoint} related with network.
     */
    private final MessageHookPointStatus rpcStatus;
    private final int batchSize;
    private final int attempt;
    private final long duration;
    private final TimeUnit timeUnit;
    private final Throwable throwable;

    MessageInterceptorContext(MessageHookPointStatus bizStatus, MessageHookPointStatus rpcStatus,
                              int batchSize, int attempt, long duration, TimeUnit timeUnit, Throwable throwable) {
        this.bizStatus = bizStatus;
        this.rpcStatus = rpcStatus;
        this.batchSize = batchSize;
        this.attempt = attempt;
        this.duration = duration;
        this.timeUnit = timeUnit;
        this.throwable = throwable;
    }

    public static Builder builder() {
        return new Builder();
    }

    public MessageHookPointStatus getBizStatus() {
        return bizStatus;
    }

    public MessageHookPointStatus getRpcStatus() {
        return rpcStatus;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public int getAttempt() {
        return this.attempt;
    }

    public long getDuration() {
        return this.duration;
    }

    public TimeUnit getTimeUnit() {
        return this.timeUnit;
    }

    public Throwable getThrowable() {
        return this.throwable;
    }

    public static class Builder {
        private MessageHookPointStatus bizStatus = MessageHookPointStatus.UNSET;
        private MessageHookPointStatus rpcStatus = MessageHookPointStatus.UNSET;
        private int messageBatchSize = 1;
        private int attempt = 1;
        private long duration = 0;
        private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        private Throwable throwable = null;

        Builder() {
        }

        public Builder setBizStatus(MessageHookPointStatus bizStatus) {
            this.bizStatus = checkNotNull(bizStatus, "bizStatus");
            return this;
        }

        public Builder setRpcStatus(MessageHookPointStatus rpcStatus) {
            this.rpcStatus = checkNotNull(rpcStatus, "rpcStatus");
            if (MessageHookPointStatus.UNSET.equals(bizStatus)) {
                setBizStatus(rpcStatus);
            }
            return this;
        }

        public Builder setMessageBatchSize(int messageBatchSize) {
            this.messageBatchSize = messageBatchSize;
            return this;
        }

        public Builder setAttempt(int attempt) {
            this.attempt = attempt;
            return this;
        }

        public Builder setDuration(long duration) {
            this.duration = duration;
            return this;
        }

        public Builder setTimeUnit(TimeUnit timeUnit) {
            this.timeUnit = checkNotNull(timeUnit, "timeUnit");
            return this;
        }

        public Builder setThrowable(Throwable throwable) {
            this.throwable = checkNotNull(throwable, "throwable");
            return this;
        }

        public MessageInterceptorContext build() {
            return new MessageInterceptorContext(bizStatus, rpcStatus, messageBatchSize, attempt, duration,
                                                 timeUnit, throwable);
        }
    }
}
