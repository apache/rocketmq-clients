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
    public static MessageInterceptorContext EMPTY = MessageInterceptorContext.builder().build();

    private final MessageHookPoint.PointStatus status;
    private final int messageBatchSize;
    private final int messageIndex;
    private final int attempt;
    private final long duration;
    private final TimeUnit timeUnit;
    private final Throwable throwable;

    MessageInterceptorContext(MessageHookPoint.PointStatus status, int messageBatchSize, int messageIndex,
                              int attempt, long duration, TimeUnit timeUnit, Throwable throwable) {
        this.status = status;
        this.messageBatchSize = messageBatchSize;
        this.messageIndex = messageIndex;
        this.attempt = attempt;
        this.duration = duration;
        this.timeUnit = timeUnit;
        this.throwable = throwable;
    }

    public static Builder builder() {
        return new Builder();
    }

    public MessageHookPoint.PointStatus getStatus() {
        return this.status;
    }

    public int getMessageBatchSize() {
        return this.messageBatchSize;
    }

    public int getMessageIndex() {
        return this.messageIndex;
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
        private MessageHookPoint.PointStatus status = MessageHookPoint.PointStatus.UNSET;
        private int messageBatchSize = 1;
        private int messageIndex = 0;
        private int attempt = 1;
        private long duration = 0;
        private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        private Throwable throwable = null;

        Builder() {
        }

        public Builder setStatus(MessageHookPoint.PointStatus status) {
            checkNotNull(status, "status");
            this.status = status;
            return this;
        }

        public Builder setMessageBatchSize(int messageBatchSize) {
            checkNotNull(status, "messageBatchSize");
            this.messageBatchSize = messageBatchSize;
            return this;
        }

        public Builder setMessageIndex(int messageIndex) {
            checkNotNull(status, "messageIndex");
            this.messageIndex = messageIndex;
            return this;
        }

        public Builder setAttempt(int attempt) {
            checkNotNull(status, "attempt");
            this.attempt = attempt;
            return this;
        }

        public Builder setDuration(long duration) {
            checkNotNull(status, "duration");
            this.duration = duration;
            return this;
        }

        public Builder setTimeUnit(TimeUnit timeUnit) {
            checkNotNull(status, "timeUnit");
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder setThrowable(Throwable throwable) {
            checkNotNull(status, "throwable");
            this.throwable = throwable;
            return this;
        }

        public MessageInterceptorContext build() {
            return new MessageInterceptorContext(status, messageBatchSize, messageIndex, attempt, duration, timeUnit,
                                                 throwable);
        }
    }
}
