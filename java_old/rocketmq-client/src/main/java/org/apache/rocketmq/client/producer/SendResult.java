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

package org.apache.rocketmq.client.producer;

import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.route.Endpoints;

public class SendResult {
    private final SendStatus sendStatus = SendStatus.SEND_OK;
    private final String msgId;
    private final Endpoints endpoints;

    private final String transactionId;

    public SendResult(Endpoints endpoints, String msgId) {
        this(endpoints, msgId, StringUtils.EMPTY);
    }

    public SendResult(Endpoints endpoints, String msgId, String transactionId) {
        this.endpoints = endpoints;
        this.msgId = msgId;
        this.transactionId = transactionId;
    }

    public SendStatus getSendStatus() {
        return this.sendStatus;
    }

    public String getMsgId() {
        return this.msgId;
    }

    public Endpoints getEndpoints() {
        return this.endpoints;
    }

    public String getTransactionId() {
        return this.transactionId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("sendStatus", sendStatus)
                          .add("msgId", msgId)
                          .add("endpoints", endpoints)
                          .toString();
    }
}
