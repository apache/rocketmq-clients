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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PublishingMessage struct {
	msg          *Message
	encoding     v2.Encoding
	messageId    string
	messageType  v2.MessageType
	traceContext *string
}

var NewPublishingMessage = func(msg *Message, settings *producerSettings, txEnabled bool) (*PublishingMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}
	pMsg := &PublishingMessage{
		msg: msg,
	}

	maxBodySizeBytes := settings.maxBodySizeBytes

	length := len(msg.Body)
	if length > maxBodySizeBytes {
		return nil, fmt.Errorf("message body size exceeds the threshold, max size=%d bytes", maxBodySizeBytes)
	}

	// No need to compress message body.
	pMsg.encoding = v2.Encoding_IDENTITY

	// Generate message id.
	pMsg.messageId = GetMessageIdCodecInstance().NextMessageId().String()
	// Normal message.
	if msg.GetMessageGroup() == nil && msg.GetDeliveryTimestamp() == nil && !txEnabled {
		pMsg.messageType = v2.MessageType_NORMAL
		return pMsg, nil
	}
	// Fifo message.
	if msg.GetMessageGroup() != nil && !txEnabled {
		pMsg.messageType = v2.MessageType_FIFO
		return pMsg, nil
	}
	// Delay message.
	if msg.GetDeliveryTimestamp() != nil && !txEnabled {
		pMsg.messageType = v2.MessageType_DELAY
		return pMsg, nil
	}
	// Transaction message.
	if msg.GetMessageGroup() == nil && msg.GetDeliveryTimestamp() == nil && txEnabled {
		pMsg.messageType = v2.MessageType_TRANSACTION
		return pMsg, nil
	}
	// Transaction semantics is conflicted with fifo/delay.
	return nil, fmt.Errorf("transactional message should not set messageGroup or deliveryTimestamp")
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
			Keys:          pMsg.msg.GetKeys(),
			MessageId:     pMsg.messageId,
			BornTimestamp: timestamppb.Now(),
			BornHost:      innerOS.Hostname(),
			BodyEncoding:  v2.Encoding_IDENTITY,
			MessageType:   pMsg.messageType,
		},
		UserProperties: pMsg.msg.GetProperties(),
		Body:           pMsg.msg.Body,
	}
	if pMsg.msg.Tag != nil {
		msg.SystemProperties.Tag = pMsg.msg.Tag
	}
	if pMsg.traceContext != nil {
		msg.SystemProperties.TraceContext = pMsg.traceContext
	}
	if pMsg.msg.GetDeliveryTimestamp() != nil {
		msg.SystemProperties.DeliveryTimestamp = timestamppb.New(*pMsg.msg.GetDeliveryTimestamp())
	}
	if pMsg.msg.messageGroup != nil {
		msg.SystemProperties.MessageGroup = pMsg.msg.messageGroup
	}
	return msg, nil
}
