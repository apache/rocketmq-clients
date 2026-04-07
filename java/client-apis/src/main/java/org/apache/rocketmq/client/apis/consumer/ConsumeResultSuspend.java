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

package org.apache.rocketmq.client.apis.consumer;

import java.time.Duration;

public class ConsumeResultSuspend extends ConsumeResult {

    private static final Duration MIN_SUSPEND_TIME = Duration.ofMillis(50);

    private final Duration suspendTime;

    private ConsumeResultSuspend(Duration suspendTime) {
        super("SUSPEND");
        if (suspendTime.compareTo(MIN_SUSPEND_TIME) < 0) {
            throw new IllegalArgumentException(String.format(
                "suspend time cannot be less than %s, got %s", MIN_SUSPEND_TIME, suspendTime));
        }
        this.suspendTime = suspendTime;
    }

    public Duration getSuspendTime() {
        return suspendTime;
    }

    public static ConsumeResultSuspend of(Duration suspendTime) {
        return new ConsumeResultSuspend(suspendTime);
    }

    @Override
    public String toString() {
        return "SUSPEND(" + suspendTime + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        ConsumeResultSuspend suspend = (ConsumeResultSuspend) o;
        return suspendTime.equals(suspend.suspendTime);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + suspendTime.hashCode();
        return result;
    }
}
