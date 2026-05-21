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

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class OffsetOption {

    public static final long POLICY_LAST_VALUE = 0L;
    public static final long POLICY_MIN_VALUE = 1L;
    public static final long POLICY_MAX_VALUE = 2L;

    public static final OffsetOption LAST_OFFSET = new OffsetOption(Type.POLICY, POLICY_LAST_VALUE);
    public static final OffsetOption MIN_OFFSET = new OffsetOption(Type.POLICY, POLICY_MIN_VALUE);
    public static final OffsetOption MAX_OFFSET = new OffsetOption(Type.POLICY, POLICY_MAX_VALUE);

    private final Type type;
    private final long value;

    private OffsetOption(Type type, long value) {
        this.type = type;
        this.value = value;
    }

    public static OffsetOption ofOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be greater than or equal to 0");
        }
        return new OffsetOption(Type.OFFSET, offset);
    }

    public static OffsetOption ofTailN(long tailN) {
        if (tailN < 0) {
            throw new IllegalArgumentException("tailN must be greater than or equal to 0");
        }
        return new OffsetOption(Type.TAIL_N, tailN);
    }

    public static OffsetOption ofTimestamp(long timestamp) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp must be greater than or equal to 0");
        }
        return new OffsetOption(Type.TIMESTAMP, timestamp);
    }

    public Type getType() {
        return type;
    }

    public long getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffsetOption option = (OffsetOption) o;
        return value == option.value && type == option.type;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(type);
        result = 31 * result + Long.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("value", value)
            .toString();
    }

    public enum Type {
        POLICY,
        OFFSET,
        TAIL_N,
        TIMESTAMP
    }

}