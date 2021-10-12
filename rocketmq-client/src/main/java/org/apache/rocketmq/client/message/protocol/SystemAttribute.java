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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.route.Endpoints;

public class SystemAttribute {
    private String tag;
    private final List<String> keys;
    private String messageId;
    private Digest digest;
    private Encoding bodyEncoding;
    private MessageType messageType;
    private long bornTimeMillis;
    private String bornHost;
    private long storeTimeMillis;
    private long deliveryTimeMillis;
    private int delayLevel;
    private String receiptHandle;
    private int partitionId;
    private long partitionOffset;
    private long invisiblePeriod;
    private int deliveryAttempt;
    private String producerGroup;
    private String messageGroup;
    private volatile String traceContext;
    private long orphanedTransactionRecoveryPeriodMillis;
    // set after receiving the message.
    private long decodedTimestamp;
    private Endpoints endpoints;

    public SystemAttribute() {
        this.tag = "";
        this.keys = new ArrayList<String>();
        this.messageId = "";
        this.messageType = MessageType.NORMAL;
        this.bornHost = "";
        this.deliveryTimeMillis = 0;
        this.delayLevel = 0;
        this.receiptHandle = "";
        this.producerGroup = "";
        this.messageGroup = "";
        this.traceContext = "";
    }

    public void setKeyList(List<String> keyList) {
        this.keys.clear();
        this.keys.addAll(keyList);
    }

    public void setKey(String key) {
        this.keys.clear();
        this.keys.add(key);
    }

    public String getTag() {
        return this.tag;
    }

    public List<String> getKeyList() {
        return this.keys;
    }

    public String getMessageId() {
        return this.messageId;
    }

    public Digest getDigest() {
        return this.digest;
    }

    public Encoding getBodyEncoding() {
        return this.bodyEncoding;
    }

    public MessageType getMessageType() {
        return this.messageType;
    }

    public long getBornTimeMillis() {
        return this.bornTimeMillis;
    }

    public String getBornHost() {
        return this.bornHost;
    }

    public long getStoreTimeMillis() {
        return storeTimeMillis;
    }

    public long getDeliveryTimeMillis() {
        return this.deliveryTimeMillis;
    }

    public int getDelayLevel() {
        return this.delayLevel;
    }

    public String getReceiptHandle() {
        return this.receiptHandle;
    }

    public int getPartitionId() {
        return this.partitionId;
    }

    public long getPartitionOffset() {
        return this.partitionOffset;
    }

    public long getInvisiblePeriod() {
        return this.invisiblePeriod;
    }

    public int getDeliveryAttempt() {
        return this.deliveryAttempt;
    }

    public String getProducerGroup() {
        return this.producerGroup;
    }

    public String getMessageGroup() {
        return this.messageGroup;
    }

    public String getTraceContext() {
        return this.traceContext;
    }

    public long getOrphanedTransactionRecoveryPeriodMillis() {
        return this.orphanedTransactionRecoveryPeriodMillis;
    }

    public long getDecodedTimestamp() {
        return this.decodedTimestamp;
    }

    public Endpoints getEndpoints() {
        return this.endpoints;
    }

    public void setTag(String tag) {
        this.tag = checkNotNull(tag, "tag");
    }

    public void setMessageId(String messageId) {
        this.messageId = checkNotNull(messageId, "messageId");
    }

    public void setDigest(Digest digest) {
        this.digest = checkNotNull(digest, "digest");
    }

    public void setBodyEncoding(Encoding bodyEncoding) {
        this.bodyEncoding = checkNotNull(bodyEncoding, "bodyEncoding");
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = checkNotNull(messageType, "messageType");
    }

    public void setBornTimeMillis(long bornTimeMillis) {
        this.bornTimeMillis = bornTimeMillis;
    }

    public void setBornHost(String bornHost) {
        this.bornHost = checkNotNull(bornHost, "bornHost");
    }

    public void setStoreTimeMillis(long storeTimeMillis) {
        this.storeTimeMillis = storeTimeMillis;
    }

    public void setDeliveryTimeMillis(long deliveryTimeMillis) {
        this.deliveryTimeMillis = deliveryTimeMillis;
    }

    public void setDelayLevel(int delayLevel) {
        this.delayLevel = delayLevel;
    }

    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = checkNotNull(receiptHandle, "receiptHandle");
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setPartitionOffset(long partitionOffset) {
        this.partitionOffset = partitionOffset;
    }

    public void setInvisiblePeriod(long invisiblePeriod) {
        this.invisiblePeriod = invisiblePeriod;
    }

    public void setDeliveryAttempt(int deliveryAttempt) {
        this.deliveryAttempt = deliveryAttempt;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = checkNotNull(producerGroup, "producerGroup");
    }

    public void setMessageGroup(String messageGroup) {
        this.messageGroup = checkNotNull(messageGroup, "messageGroup");
    }

    public void setTraceContext(String traceContext) {
        this.traceContext = checkNotNull(traceContext, "traceContext");
    }

    public void setOrphanedTransactionRecoveryPeriodMillis(long orphanedTransactionRecoveryPeriodMillis) {
        this.orphanedTransactionRecoveryPeriodMillis = orphanedTransactionRecoveryPeriodMillis;
    }

    public void setDecodedTimestamp(long decodedTimestamp) {
        this.decodedTimestamp = decodedTimestamp;
    }

    public void setEndpoints(Endpoints endpoints) {
        this.endpoints = checkNotNull(endpoints, "endpoints");
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("tag", tag)
                          .add("keys", keys)
                          .add("messageId", messageId)
                          .add("digest", digest)
                          .add("bodyEncoding", bodyEncoding)
                          .add("messageType", messageType)
                          .add("bornTimeMillis", bornTimeMillis)
                          .add("bornHost", bornHost)
                          .add("storeTimeMillis", storeTimeMillis)
                          .add("deliveryTimeMillis", deliveryTimeMillis)
                          .add("delayLevel", delayLevel)
                          .add("receiptHandle", receiptHandle)
                          .add("partitionId", partitionId)
                          .add("partitionOffset", partitionOffset)
                          .add("invisiblePeriod", invisiblePeriod)
                          .add("deliveryAttempt", deliveryAttempt)
                          .add("producerGroup", producerGroup)
                          .add("messageGroup", messageGroup)
                          .add("traceContext", traceContext)
                          .add("orphanedTransactionRecoveryPeriodMillis", orphanedTransactionRecoveryPeriodMillis)
                          .add("decodedTimestamp", decodedTimestamp)
                          .add("endpoints", endpoints)
                          .toString();
    }
}
