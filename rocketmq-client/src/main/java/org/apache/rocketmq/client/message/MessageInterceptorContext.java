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
    private final Builder builder;
    private final MessageHookPointStatus status;
    private final String topic;
    private final int batchSize;
    private final int attempt;
    private final long duration;
    private final TimeUnit timeUnit;
    private final Throwable throwable;

    MessageInterceptorContext(Builder builder, MessageHookPointStatus status, String topic, int batchSize, int attempt,
                              long duration, TimeUnit timeUnit, Throwable throwable) {
        this.builder = builder;
        this.status = status;
        this.topic = topic;
        this.batchSize = batchSize;
        this.attempt = attempt;
        this.duration = duration;
        this.timeUnit = timeUnit;
        this.throwable = throwable;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return builder;
    }

    public MessageHookPointStatus getStatus() {
        return status;
    }

    public String getTopic() {
        return topic;
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
        private MessageHookPointStatus status = MessageHookPointStatus.UNSET;
        private String topic;
        private int batchSize = 1;
        private int attempt = 1;
        private long duration = 0;
        private TimeUnit timeUnit = MessageInterceptor.DEFAULT_TIME_UNIT;
        private Throwable throwable = null;

        Builder() {
        }

        public Builder setStatus(MessageHookPointStatus status) {
            this.status = checkNotNull(status, "status");
            return this;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
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
            return new MessageInterceptorContext(this, status, topic, batchSize, attempt, duration,
                                                 timeUnit, throwable);
        }
    }
}
