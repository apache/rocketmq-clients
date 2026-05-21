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

import org.apache.rocketmq.client.apis.consumer.OffsetOption;

public class ProtobufUtils {
    private ProtobufUtils() {
    }

    public static apache.rocketmq.v2.OffsetOption toProtobufOffsetOption(OffsetOption offsetOption) {
        apache.rocketmq.v2.OffsetOption.Builder protoBuilder = apache.rocketmq.v2.OffsetOption.newBuilder();
        switch (offsetOption.getType()) {
            case POLICY:
                protoBuilder.setPolicy(toProtobufPolicy(offsetOption.getValue()));
                break;
            case OFFSET:
                protoBuilder.setOffset(offsetOption.getValue());
                break;
            case TAIL_N:
                protoBuilder.setTailN(offsetOption.getValue());
                break;
            case TIMESTAMP:
                protoBuilder.setTimestamp(offsetOption.getValue());
                break;
            default:
                throw new IllegalArgumentException("Unknown OffsetOption type: " + offsetOption.getType());
        }
        return protoBuilder.build();
    }

    public static apache.rocketmq.v2.OffsetOption.Policy toProtobufPolicy(long policyValue) {
        if (policyValue == OffsetOption.POLICY_LAST_VALUE) {
            return apache.rocketmq.v2.OffsetOption.Policy.LAST;
        } else if (policyValue == OffsetOption.POLICY_MIN_VALUE) {
            return apache.rocketmq.v2.OffsetOption.Policy.MIN;
        } else if (policyValue == OffsetOption.POLICY_MAX_VALUE) {
            return apache.rocketmq.v2.OffsetOption.Policy.MAX;
        }
        throw new IllegalArgumentException("Unknown policy type: " + policyValue);
    }

}
