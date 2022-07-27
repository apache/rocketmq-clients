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

import (
	"fmt"

	innerOS "github.com/apache/rocketmq-clients/golang/pkg/os"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PublishingMessage struct {
	msg         *Message
	encoding    v2.Encoding
	messageId   string
	messageType v2.MessageType
}

// TODO need ProducerSettings
func NewPublishingMessage(msg *Message, txEnabled bool) (*PublishingMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}
	pMsg := &PublishingMessage{
		msg: msg,
	}

	maxBodySizeBytes := 4 * 1024 * 1024

	length := len(msg.Body)
	if length > maxBodySizeBytes {
		return nil, fmt.Errorf("message body size exceeds the threshold, max size=%d bytes", maxBodySizeBytes)
	}

	// No need to compress message body.
	pMsg.encoding = v2.Encoding_IDENTITY

	// Generate message id.
	// TODO there is 'MessageIdCodec.getInstance().nextMessageId()' in client-java
	pMsg.messageId = uuid.New().String()
	// Normal message.
	if len(msg.GetMessageGroup()) == 0 && msg.GetDeliveryTimestamp().IsZero() && !txEnabled {
		pMsg.messageType = v2.MessageType_NORMAL
		return pMsg, nil
	}
	// Fifo message.
	if len(msg.GetMessageGroup()) != 0 && !txEnabled {
		pMsg.messageType = v2.MessageType_FIFO
		return pMsg, nil
	}
	// Delay message.
	if !msg.GetDeliveryTimestamp().IsZero() && !txEnabled {
		pMsg.messageType = v2.MessageType_DELAY
		return pMsg, nil
	}
	// Transaction message.
	if len(msg.GetMessageGroup()) == 0 && msg.GetDeliveryTimestamp().IsZero() && txEnabled {
		pMsg.messageType = v2.MessageType_TRANSACTION
		return pMsg, nil
	}
	// Transaction semantics is conflicted with fifo/delay.
	return nil, fmt.Errorf("Transactional message should not set messageGroup or deliveryTimestamp")
}

func (pMsg *PublishingMessage) toProtobuf() (*v2.Message, error) {
	if pMsg == nil {
		return nil, fmt.Errorf("publishingMessage is nil")
	}
	msg := &v2.Message{
		Topic: &v2.Resource{
			// ResourceNamespace: b.conn.Config().NameSpace,
			Name: pMsg.msg.Topic,
		},
		SystemProperties: &v2.SystemProperties{
			Keys:          pMsg.msg.Keys,
			MessageId:     uuid.New().String(),
			BornTimestamp: timestamppb.Now(),
			BornHost:      innerOS.Hostname(),
			BodyEncoding:  v2.Encoding_IDENTITY,
			MessageType:   v2.MessageType_NORMAL,
		},
		UserProperties: pMsg.msg.Properties,
		Body:           pMsg.msg.Body,
	}
	if len(pMsg.msg.Tag) > 0 {
		msg.SystemProperties.Tag = &pMsg.msg.Tag
	}
	if !pMsg.msg.GetDeliveryTimestamp().IsZero() {
		msg.SystemProperties.DeliveryTimestamp = timestamppb.New(pMsg.msg.GetDeliveryTimestamp())
	}
	if len(pMsg.msg.messageGroup) > 0 {
		msg.SystemProperties.MessageGroup = &pMsg.msg.messageGroup
	}
	return msg, nil
}
