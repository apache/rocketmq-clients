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

import "time"

type Message struct {
	Topic      string
	Body       []byte
	Tag        string
	Keys       []string
	Properties map[string]string

	deliveryTimestamp time.Time
	messageGroup      string
}

type MessageExt struct {
	MessageID     string
	ReceiptHandle string
	Message
}

type SendReceipt struct {
	MessageID string
}

func (msg *Message) SetDelayTimeLevel(deliveryTimestamp time.Time) {
	msg.deliveryTimestamp = deliveryTimestamp
}

func (msg *Message) GetDeliveryTimestamp() time.Time {
	return msg.deliveryTimestamp
}

func (msg *Message) SetMessageGroup(messageGroup string) {
	msg.messageGroup = messageGroup
}

func (msg *Message) GetMessageGroup() string {
	return msg.messageGroup
}
