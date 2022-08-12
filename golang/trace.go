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

package golang

// RocketMQ span attribute name list
const (
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_OPERATION           = "messaging.rocketmq.operation"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_NAMESPACE           = "messaging.rocketmq.namespace"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_TAG                 = "messaging.rocketmq.message_tag"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_KEYS                = "messaging.rocketmq.message_keys"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_CLIENT_ID           = "messaging.rocketmq.client_id"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_MESSAGE_TYPE        = "messaging.rocketmq.message_type"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_CLIENT_GROUP        = "messaging.rocketmq.client_group"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_ATTEMPT             = "messaging.rocketmq.attempt"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_BATCH_SIZE          = "messaging.rocketmq.batch_size"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_DELIVERY_TIMESTAMP  = "messaging.rocketmq.delivery_timestamp"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_AVAILABLE_TIMESTAMP = "messaging.rocketmq.available_timestamp"
	SPAN_ATTRIBUTE_KEY_ROCKETMQ_ACCESS_KEY          = "messaging.rocketmq.access_key"

	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_MESSAGING_SYSTEM  = "rocketmq"
	SPAN_ATTRIBUTE_VALUE_DESTINATION_KIND           = "topic"
	SPAN_ATTRIBUTE_VALUE_MESSAGING_PROTOCOL         = "RMQ-gRPC"
	SPAN_ATTRIBUTE_VALUE_MESSAGING_PROTOCOL_VERSION = "v1"

	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_NORMAL_MESSAGE      = "normal"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_FIFO_MESSAGE        = "fifo"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_DELAY_MESSAGE       = "delay"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_TRANSACTION_MESSAGE = "transaction"

	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_SEND_OPERATION     = "send"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_RECEIVE_OPERATION  = "receive"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_PULL_OPERATION     = "pull"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_AWAIT_OPERATION    = "await"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_PROCESS_OPERATION  = "process"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_ACK_OPERATION      = "ack"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_NACK_OPERATION     = "nack"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_COMMIT_OPERATION   = "commit"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_ROLLBACK_OPERATION = "rollback"
	SPAN_ATTRIBUTE_VALUE_ROCKETMQ_DLQ_OPERATION      = "dlq"

	// Messaging span attribute name list
	SPAN_ATTRIBUTE_KEY_MESSAGING_SYSTEM             = "messaging.system"
	SPAN_ATTRIBUTE_KEY_MESSAGING_DESTINATION        = "messaging.destination"
	SPAN_ATTRIBUTE_KEY_MESSAGING_DESTINATION_KIND   = "messaging.destination_kind"
	SPAN_ATTRIBUTE_KEY_MESSAGING_PROTOCOL           = "messaging.protocol"
	SPAN_ATTRIBUTE_KEY_MESSAGING_PROTOCOL_VERSION   = "messaging.protocol_version"
	SPAN_ATTRIBUTE_KEY_MESSAGING_URL                = "messaging.url"
	SPAN_ATTRIBUTE_KEY_MESSAGING_ID                 = "messaging.message_id"
	SPAN_ATTRIBUTE_KEY_MESSAGING_PAYLOAD_SIZE_BYTES = "messaging.message_payload_size_bytes"
	SPAN_ATTRIBUTE_KEY_MESSAGING_OPERATION          = "messaging.operation"

	SPAN_ATTRIBUTE_VALUE_MESSAGING_SEND_OPERATION    = "send"
	SPAN_ATTRIBUTE_VALUE_MESSAGING_RECEIVE_OPERATION = "receive"
	SPAN_ATTRIBUTE_VALUE_MESSAGING_PROCESS_OPERATION = "process"

	SPAN_ATTRIBUTE_KEY_TRANSACTION_RESOLUTION = "commitAction"

	// Span annotation
	SPAN_ANNOTATION_AWAIT_CONSUMPTION = "__await_consumption"
	SPAN_ANNOTATION_MESSAGE_KEYS      = "__message_keys"
	SPAN_ANNOTATION_ATTR_START_TIME   = "__start_time"
)
