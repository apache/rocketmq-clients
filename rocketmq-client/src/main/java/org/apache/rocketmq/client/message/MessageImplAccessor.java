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

import apache.rocketmq.v1.SystemAttribute;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.message.protocol.Digest;
import org.apache.rocketmq.client.message.protocol.DigestType;
import org.apache.rocketmq.client.message.protocol.Encoding;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.utility.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageImplAccessor {
    private static final Logger log = LoggerFactory.getLogger(MessageImplAccessor.class);

    private MessageImplAccessor() {
    }

    public static MessageImpl getMessageImpl(Message message) {
        return message.impl;
    }

    public static MessageImpl getMessageImpl(MessageExt messageExt) {
        return messageExt.impl;
    }

    public static MessageImpl wrapMessageImpl(apache.rocketmq.v1.Message message) {
        final org.apache.rocketmq.client.message.protocol.SystemAttribute mqSystemAttribute =
                new org.apache.rocketmq.client.message.protocol.SystemAttribute();
        final SystemAttribute systemAttribute = message.getSystemAttribute();
        // tag.
        mqSystemAttribute.setTag(systemAttribute.getTag());
        // keyList.
        List<String> keyList = new ArrayList<String>(systemAttribute.getKeysList());
        mqSystemAttribute.setKeyList(keyList);
        // message id.
        mqSystemAttribute.setMessageId(systemAttribute.getMessageId());
        // digest.
        final apache.rocketmq.v1.Digest bodyDigest = systemAttribute.getBodyDigest();
        byte[] body = message.getBody().toByteArray();
        boolean corrupted = false;
        String expectedCheckSum;
        DigestType digestType = DigestType.CRC32;
        final String checksum = bodyDigest.getChecksum();
        switch (bodyDigest.getType()) {
            case CRC32:
                expectedCheckSum = UtilAll.crc32CheckSum(body);
                if (!expectedCheckSum.equals(checksum)) {
                    corrupted = true;
                }
                break;
            case MD5:
                try {
                    expectedCheckSum = UtilAll.md5CheckSum(body);
                    if (!expectedCheckSum.equals(checksum)) {
                        corrupted = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    corrupted = true;
                    log.warn("MD5 is not supported unexpectedly, skip it.");
                }
                break;
            case SHA1:
                try {
                    expectedCheckSum = UtilAll.sha1CheckSum(body);
                    if (!expectedCheckSum.equals(checksum)) {
                        corrupted = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    corrupted = true;
                    log.warn("SHA-1 is not supported unexpectedly, skip it.");
                }
                break;
            default:
                log.warn("Unsupported message body digest algorithm.");
        }
        mqSystemAttribute.setDigest(new Digest(digestType, checksum));

        switch (systemAttribute.getBodyEncoding()) {
            case GZIP:
                try {
                    body = UtilAll.uncompressBytesGzip(body);
                    mqSystemAttribute.setBodyEncoding(Encoding.GZIP);
                } catch (IOException e) {
                    log.error("Failed to uncompress message body, messageId={}", systemAttribute.getMessageId());
                    corrupted = true;
                }
                break;
            case IDENTITY:
                mqSystemAttribute.setBodyEncoding(Encoding.IDENTITY);
                break;
            default:
                log.warn("Unsupported message encoding algorithm.");
        }

        // message type.
        MessageType messageType;
        switch (systemAttribute.getMessageType()) {
            case NORMAL:
                messageType = MessageType.NORMAL;
                break;
            case FIFO:
                messageType = MessageType.FIFO;
                break;
            case DELAY:
                messageType = MessageType.DELAY;
                break;
            case TRANSACTION:
                messageType = MessageType.TRANSACTION;
                break;
            default:
                messageType = MessageType.NORMAL;
                log.warn("Unknown message type, fall through to normal type");
        }
        mqSystemAttribute.setMessageType(messageType);
        // born time millis.
        mqSystemAttribute.setBornTimeMillis(Timestamps.toMillis(systemAttribute.getBornTimestamp()));
        // born host.
        mqSystemAttribute.setBornHost(systemAttribute.getBornHost());
        // store time millis
        mqSystemAttribute.setStoreTimeMillis(Timestamps.toMillis(systemAttribute.getStoreTimestamp()));

        switch (systemAttribute.getTimedDeliveryCase()) {
            case DELAY_LEVEL:
                // delay level
                mqSystemAttribute.setDelayLevel(systemAttribute.getDelayLevel());
                break;
            case DELIVERY_TIMESTAMP:
                // delay timestamp
                mqSystemAttribute.setDeliveryTimeMillis(Timestamps.toMillis(systemAttribute.getDeliveryTimestamp()));
                break;
            case TIMEDDELIVERY_NOT_SET:
            default:
                break;
        }
        // receipt handle.
        mqSystemAttribute.setReceiptHandle(systemAttribute.getReceiptHandle());
        // partition id.
        mqSystemAttribute.setPartitionId(systemAttribute.getPartitionId());
        // partition offset.
        mqSystemAttribute.setPartitionOffset(systemAttribute.getPartitionOffset());
        // invisible period.
        mqSystemAttribute.setInvisiblePeriod(Durations.toMillis(systemAttribute.getInvisiblePeriod()));
        // delivery attempt
        mqSystemAttribute.setDeliveryAttempt(systemAttribute.getDeliveryAttempt());
        // producer group.
        mqSystemAttribute.setProducerGroup(systemAttribute.getProducerGroup().getName());
        // message group.
        mqSystemAttribute.setMessageGroup(systemAttribute.getMessageGroup());
        // trace context.
        mqSystemAttribute.setTraceContext(systemAttribute.getTraceContext());
        // transaction resolve delay millis.
        mqSystemAttribute.setOrphanedTransactionRecoveryPeriodMillis(
                Durations.toMillis(systemAttribute.getOrphanedTransactionRecoveryPeriod()));
        // decoded timestamp.
        mqSystemAttribute.setDecodedTimestamp(System.currentTimeMillis());
        // user properties.
        final ConcurrentHashMap<String, String> mqUserAttribute =
                new ConcurrentHashMap<String, String>(message.getUserAttributeMap());

        final String topic = message.getTopic().getName();
        return new MessageImpl(topic, mqSystemAttribute, mqUserAttribute, body, corrupted);
    }
}
