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

package org.apache.rocketmq.client.java.misc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.junit.Test;

public class ProtobufUtilsTest {
    @Test
    public void testToProtobufOffsetOptionWithPolicy() {
        OffsetOption offsetOption = OffsetOption.LAST_OFFSET;
        apache.rocketmq.v2.OffsetOption protobufOffsetOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protobufOffsetOption.hasPolicy()).isTrue();
        assertThat(protobufOffsetOption.getPolicy()).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.LAST);
        assertThat(protobufOffsetOption.hasOffset()).isFalse();
        assertThat(protobufOffsetOption.hasTailN()).isFalse();
        assertThat(protobufOffsetOption.hasTimestamp()).isFalse();
    }

    @Test
    public void testToProtobufOffsetOptionWithOffset() {
        long offsetValue = 100L;
        OffsetOption offsetOption = OffsetOption.ofOffset(offsetValue);
        apache.rocketmq.v2.OffsetOption protobufOffsetOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protobufOffsetOption.hasOffset()).isTrue();
        assertThat(protobufOffsetOption.getOffset()).isEqualTo(offsetValue);
        assertThat(protobufOffsetOption.hasPolicy()).isFalse();
        assertThat(protobufOffsetOption.hasTailN()).isFalse();
        assertThat(protobufOffsetOption.hasTimestamp()).isFalse();
    }

    @Test
    public void testToProtobufOffsetOptionWithTailN() {
        long tailNValue = 5L;
        OffsetOption offsetOption = OffsetOption.ofTailN(tailNValue);
        apache.rocketmq.v2.OffsetOption protobufOffsetOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protobufOffsetOption.hasTailN()).isTrue();
        assertThat(protobufOffsetOption.getTailN()).isEqualTo(tailNValue);
        assertThat(protobufOffsetOption.hasPolicy()).isFalse();
        assertThat(protobufOffsetOption.hasOffset()).isFalse();
        assertThat(protobufOffsetOption.hasTimestamp()).isFalse();
    }

    @Test
    public void testToProtobufOffsetOptionWithTimestamp() {
        long timestampValue = System.currentTimeMillis();
        OffsetOption offsetOption = OffsetOption.ofTimestamp(timestampValue);
        apache.rocketmq.v2.OffsetOption protobufOffsetOption = ProtobufUtils.toProtobufOffsetOption(offsetOption);
        assertThat(protobufOffsetOption.hasTimestamp()).isTrue();
        assertThat(protobufOffsetOption.getTimestamp()).isEqualTo(timestampValue);
        assertThat(protobufOffsetOption.hasPolicy()).isFalse();
        assertThat(protobufOffsetOption.hasOffset()).isFalse();
        assertThat(protobufOffsetOption.hasTailN()).isFalse();
    }

    @Test
    public void testToProtobufPolicyWithLast() {
        long policyValue = OffsetOption.POLICY_LAST_VALUE;
        apache.rocketmq.v2.OffsetOption.Policy policy = ProtobufUtils.toProtobufPolicy(policyValue);
        assertThat(policy).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.LAST);
    }

    @Test
    public void testToProtobufPolicyWithMin() {
        long policyValue = OffsetOption.POLICY_MIN_VALUE;
        apache.rocketmq.v2.OffsetOption.Policy policy = ProtobufUtils.toProtobufPolicy(policyValue);
        assertThat(policy).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.MIN);
    }

    @Test
    public void testToProtobufPolicyWithMax() {
        long policyValue = OffsetOption.POLICY_MAX_VALUE;
        apache.rocketmq.v2.OffsetOption.Policy policy = ProtobufUtils.toProtobufPolicy(policyValue);
        assertThat(policy).isEqualTo(apache.rocketmq.v2.OffsetOption.Policy.MAX);
    }

    @Test
    public void testToProtobufPolicyWithUnknownValue() {
        long unknownPolicyValue = 999L;
        assertThatThrownBy(() -> ProtobufUtils.toProtobufPolicy(unknownPolicyValue))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown policy type");
    }
}