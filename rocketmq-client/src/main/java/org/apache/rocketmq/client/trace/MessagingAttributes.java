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

public class MessagingAttributes {

    public static final class MessagingDestinationKindValues {
        /**
         * A message sent to a queue.
         */
        public static final String QUEUE = "queue";
        /**
         * A message sent to a topic.
         */
        public static final String TOPIC = "topic";

        private MessagingDestinationKindValues() {
        }
    }

    public static final class MessagingOperationValues {
        /**
         * receive.
         */
        public static final String RECEIVE = "receive";
        /**
         * process.
         */
        public static final String PROCESS = "process";

        private MessagingOperationValues() {
        }
    }

    /**
     * A string identifying the messaging system.
     */
    public static final AttributeKey<String> MESSAGING_SYSTEM = stringKey("messaging.system");
    /**
     * The message destination name. This might be equal to the span name but is required
     * nevertheless.
     */
    public static final AttributeKey<String> MESSAGING_DESTINATION = stringKey("messaging.destination");
    /**
     * The kind of message destination.
     */
    public static final AttributeKey<String> MESSAGING_DESTINATION_KIND = stringKey("messaging.destination_kind");
    /**
     * The name of the transport protocol.
     */
    public static final AttributeKey<String> MESSAGING_PROTOCOL = stringKey("messaging.protocol");

    /**
     * The version of the transport protocol.
     */
    public static final AttributeKey<String> MESSAGING_PROTOCOL_VERSION = stringKey("messaging.protocol_version");

    /**
     * Connection string.
     */
    public static final AttributeKey<String> MESSAGING_URL = stringKey("messaging.url");

    /**
     * A value used by the messaging system as an identifier for the message, represented as a string.
     */
    public static final AttributeKey<String> MESSAGING_MESSAGE_ID = stringKey("messaging.message_id");
    /**
     * The (uncompressed) size of the message payload in bytes. Also use this attribute if it is
     * unknown whether the compressed or uncompressed payload size is reported.
     */
    public static final AttributeKey<Long> MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES =
            longKey("messaging.message_payload_size_bytes");

    /**
     * A string identifying the kind of message consumption as defined in the [Operation
     * names](#operation-names) section above. If the operation is &#34;send&#34;, this attribute MUST
     * NOT be set, since the operation can be inferred from the span kind in that case.
     */
    public static final AttributeKey<String> MESSAGING_OPERATION = stringKey("messaging.operation");

    private MessagingAttributes() {
    }
}
