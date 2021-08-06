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

package org.apache.rocketmq.client.message.protocol;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.rocketmq.client.route.Endpoints;

@Data
public class SystemAttribute {
    private String tag;
    private final List<String> keys;
    private String messageId;
    private Digest digest;
    private Encoding bodyEncoding;
    private MessageType messageType;
    private long bornTimeMillis;
    private String bornHost;
    private long deliveryTimeMillis;
    private int delayLevel;
    private String receiptHandle;
    private int partitionId;
    private long partitionOffset;
    private long invisiblePeriod;
    private int deliveryAttempt;
    private String producerGroup;
    private String messageGroup;
    private String traceContext;
    private long orphanedTransactionRecoveryPeriodMillis;
    // Would set after receiving the message.
    private long decodedTimestamp;
    private Endpoints ackEndpoints;

    public SystemAttribute() {
        this.keys = new ArrayList<String>();
        this.messageType = MessageType.NORMAL;
        this.deliveryTimeMillis = 0;
        this.delayLevel = 0;
    }

    public void setKeys(List<String> keys) {
        this.keys.clear();
        this.keys.addAll(keys);
    }

    public void setKey(String key) {
        this.keys.clear();
        this.keys.add(key);
    }
}
