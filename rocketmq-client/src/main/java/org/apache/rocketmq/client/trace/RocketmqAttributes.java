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


package org.apache.rocketmq.client.trace;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

import io.opentelemetry.api.common.AttributeKey;

public class RocketmqAttributes {

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_OPERATION = stringKey("messaging.rocketmq.operation");

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_NAMESPACE = stringKey("messaging.rocketmq.namespace");

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_MESSAGE_TAG =
            stringKey("messaging.rocketmq.message_tag");

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_MESSAGE_KEYS =
            stringKey("messaging.rocketmq.message_keys");

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_CLIENT_ID = stringKey("messaging.rocketmq.client_id");

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_MESSAGE_TYPE =
            stringKey("messaging.rocketmq.message_type");

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_CLIENT_GROUP =
            stringKey("messaging.rocketmq.client_group");

    public static final AttributeKey<Long> MESSAGING_ROCKETMQ_ATTEMPT = longKey("messaging.rocketmq.attempt");

    public static final AttributeKey<Long> MESSAGING_ROCKETMQ_BATCH_SIZE = longKey("messaging.rocketmq.batch_size");

    public static final AttributeKey<Long> MESSAGING_ROCKETMQ_DELIVERY_TIMESTAMP =
            longKey("messaging.rocketmq.delivery_timestamp");

    public static final AttributeKey<Long> MESSAGING_ROCKETMQ_AVAILABLE_TIMESTAMP =
            longKey("messaging.rocketmq.available_timestamp");

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_VERSION =
            stringKey("messaging.rocketmq.version");

    public static final AttributeKey<String> MESSAGING_ROCKETMQ_ACCESS_KEY = stringKey("messaging.rocketmq.access_key");

    private RocketmqAttributes() {
    }
}
