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
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"hash/crc32"
	"strconv"
	"time"

	"github.com/apache/rocketmq-clients/golang/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UnifiedMessage struct {
	msg    *Message
	pubMsg *PublishingMessage
}

func (uMsg *UnifiedMessage) GetMessage() *Message {
	if uMsg.pubMsg != nil {
		return uMsg.pubMsg.msg
	}
	return uMsg.msg
}

type MessageHookPointsStatus int32

const (
	MessageHookPointsStatus_UNSET MessageHookPointsStatus = iota
	MessageHookPointsStatus_OK
	MessageHookPointsStatus_ERROR
)

type MessageHookPoints int32

const (
	MessageHookPoints_SEND MessageHookPoints = iota
	MessageHookPoints_RECEIVE
	MessageHookPoints_CONSUME
	MessageHookPoints_ACK
	MessageHookPoints_CHANGE_INVISIBLE_DURATION
	MessageHookPoints_COMMIT_TRANSACTION
	MessageHookPoints_ROLLBACK_TRANSACTION
	MessageHookPoints_FORWARD_TO_DLQ
)

type MessageInterceptor interface {
	doBefore(messageHookPoints MessageHookPoints, messageCommons []*MessageCommon) error
	doAfter(messageHookPoints MessageHookPoints, messageCommons []*MessageCommon, duration time.Duration, status MessageHookPointsStatus) error
}

type Message struct {
	Topic        string
	Body         []byte
	Tag          *string
	messageGroup *string
	keys         []string
	properties   map[string]string

	deliveryTimestamp  *time.Time
	parentTraceContext *string
}

type SendReceipt struct {
	MessageID     string
	TransactionId string
	Offset        int64
	Endpoints     *v2.Endpoints
}

func (msg *Message) SetTag(tag string) {
	msg.Tag = &tag
}

func (msg *Message) GetTag() *string {
	return msg.Tag
}

func (msg *Message) GetKeys() []string {
	return msg.keys
}

func (msg *Message) SetKeys(keys ...string) {
	msg.keys = keys
}

func (msg *Message) GetProperties() map[string]string {
	return msg.properties
}

func (msg *Message) SetDelayTimestamp(deliveryTimestamp time.Time) {
	msg.deliveryTimestamp = &deliveryTimestamp
}

func (msg *Message) GetDeliveryTimestamp() *time.Time {
	return msg.deliveryTimestamp
}

func (msg *Message) SetMessageGroup(messageGroup string) {
	msg.messageGroup = &messageGroup
}

func (msg *Message) GetMessageGroup() *string {
	return msg.messageGroup
}

func (msg *Message) GetMessageCommon() *MessageCommon {
	return &MessageCommon{
		topic:              msg.Topic,
		body:               msg.Body,
		tag:                msg.Tag,
		messageGroup:       msg.messageGroup,
		deliveryTimestamp:  msg.deliveryTimestamp,
		parentTraceContext: msg.parentTraceContext,
		keys:               msg.keys,
		properties:         msg.properties,
	}
}

type MessageCommon struct {
	messageId                   *string
	topic                       string
	body                        []byte
	properties                  map[string]string
	tag                         *string
	keys                        []string
	messageGroup                *string
	deliveryTimestamp           *time.Time
	bornHost                    *string
	parentTraceContext          *string
	traceContext                *string
	bornTimestamp               *time.Time
	deliveryAttempt             int32
	decodeStopwatch             *time.Time
	deliveryTimestampFromRemote *timestamppb.Timestamp
}

type MessageView struct {
	messageId                   string
	topic                       string
	body                        []byte
	properties                  map[string]string
	tag                         *string
	keys                        []string
	messageGroup                *string
	deliveryTimestamp           *time.Time
	bornHost                    *string
	traceContext                *string
	bornTimestamp               *time.Time
	messageQueue                *v2.MessageQueue
	endpoints                   *v2.Endpoints
	deliveryAttempt             int32
	decodeStopwatch             *time.Time
	deliveryTimestampFromRemote *timestamppb.Timestamp

	offset        int64
	ReceiptHandle string
	corrupted     bool
}

func fromProtobuf_MessageView0(message *v2.Message) *MessageView {
	return fromProtobuf_MessageView1(message, nil)
}
func fromProtobuf_MessageView1(message *v2.Message, messageQueue *v2.MessageQueue) *MessageView {
	return fromProtobuf_MessageView2(message, messageQueue, nil)
}
func fromProtobuf_MessageView2(message *v2.Message, messageQueue *v2.MessageQueue, deliveryTimestampFromRemote *timestamppb.Timestamp) *MessageView {
	systemProperties := message.GetSystemProperties()
	mv := &MessageView{
		topic:     message.GetTopic().GetName(),
		messageId: systemProperties.GetMessageId(),
		body:      message.GetBody(),
	}
	bodyDigest := systemProperties.GetBodyDigest()
	corrupted := false
	checksum := bodyDigest.GetChecksum()
	var expectedChecksum string
	switch bodyDigest.GetType() {
	case v2.DigestType_CRC32:
		expectedChecksum = strconv.FormatInt(int64(crc32.ChecksumIEEE(message.GetBody())), 16)
		if expectedChecksum != checksum {
			corrupted = true
		}
	case v2.DigestType_MD5:
		c := md5.New()
		c.Write(message.GetBody())
		expectedChecksum = hex.EncodeToString(c.Sum(nil))
		if expectedChecksum != checksum {
			corrupted = true
		}
	case v2.DigestType_SHA1:
		c := sha1.New()
		c.Write(message.GetBody())
		expectedChecksum = hex.EncodeToString(c.Sum(nil))
		if expectedChecksum != checksum {
			corrupted = true
		}
	default:
		sugarBaseLogger.Warnf("unsupported message body digest algorithm, digestType=%v, topic=%s, messageId=%s", bodyDigest.GetType(), mv.topic, mv.messageId)
	}
	bodyEncoding := systemProperties.GetBodyEncoding()
	switch bodyEncoding {
	case v2.Encoding_GZIP:
		unCompressBody, err := utils.GZIPDecode(message.GetBody())
		if err != nil {
			sugarBaseLogger.Errorf("failed to uncompress message body, topic=%s, messageId=%s, err=%w", mv.topic, mv.messageId, err)
			corrupted = true
		} else {
			mv.body = unCompressBody
		}
	case v2.Encoding_IDENTITY:
		break
	default:
		sugarBaseLogger.Errorf("unsupported message encoding algorithm, topic=%s, messageId=%s, bodyEncoding=%v", mv.topic, mv.messageId, bodyEncoding)
	}
	mv.tag = systemProperties.Tag
	mv.messageGroup = systemProperties.MessageGroup

	mv.keys = systemProperties.GetKeys()
	mv.bornHost = &systemProperties.BornHost
	mv.deliveryAttempt = systemProperties.GetDeliveryAttempt()
	mv.messageQueue = messageQueue
	if messageQueue != nil {
		mv.endpoints = messageQueue.Broker.GetEndpoints()
	}
	mv.offset = systemProperties.GetQueueOffset()
	mv.properties = message.GetUserProperties()
	mv.ReceiptHandle = systemProperties.GetReceiptHandle()
	mv.traceContext = systemProperties.TraceContext
	mv.corrupted = corrupted
	if systemProperties.GetDeliveryTimestamp() != nil {
		deliveryTimestamp := systemProperties.GetDeliveryTimestamp().AsTime()
		mv.deliveryTimestamp = &deliveryTimestamp
	}
	if systemProperties.GetBornTimestamp() != nil {
		bornTimestamp := systemProperties.GetBornTimestamp().AsTime()
		mv.bornTimestamp = &bornTimestamp
	}
	mv.deliveryTimestampFromRemote = deliveryTimestampFromRemote
	return mv
}

func (mv *MessageView) isCorrupted() bool {
	return mv.corrupted
}

func (msg *MessageView) GetMessageCommon() *MessageCommon {
	return &MessageCommon{
		messageId:                   &msg.messageId,
		topic:                       msg.topic,
		body:                        msg.body,
		tag:                         msg.tag,
		messageGroup:                msg.messageGroup,
		deliveryTimestamp:           msg.deliveryTimestamp,
		keys:                        msg.keys,
		properties:                  msg.properties,
		bornHost:                    msg.bornHost,
		traceContext:                msg.traceContext,
		bornTimestamp:               msg.bornTimestamp,
		deliveryAttempt:             msg.deliveryAttempt,
		decodeStopwatch:             msg.decodeStopwatch,
		deliveryTimestampFromRemote: msg.deliveryTimestampFromRemote,
	}
}

func (msg *MessageView) GetMessageId() string {
	return msg.messageId
}

func (msg *MessageView) GetTopic() string {
	return msg.topic
}

func (msg *MessageView) GetBody() []byte {
	return msg.body
}

func (msg *MessageView) GetProperties() map[string]string {
	return msg.properties
}

func (msg *MessageView) GetTag() *string {
	return msg.tag
}

func (msg *MessageView) GetKeys() []string {
	return msg.keys
}

func (msg *MessageView) GetMessageGroup() *string {
	return msg.messageGroup
}

func (msg *MessageView) SetTag(tag string) {
	msg.tag = &tag
}

func (msg *MessageView) GetDeliveryTimestamp() *time.Time {
	return msg.deliveryTimestamp
}

func (msg *MessageView) GetBornHost() *string {
	return msg.bornHost
}

func (msg *MessageView) GetBornTimestamp() *time.Time {
	return msg.bornTimestamp
}

func (msg *MessageView) GetDeliveryAttempt() int32 {
	return msg.deliveryAttempt
}

func (msg *MessageView) GetTraceContext() *string {
	return msg.traceContext
}

func (msg *MessageView) GetReceiptHandle() string {
	return msg.ReceiptHandle
}

func (msg *MessageView) GetOffset() int64 {
	return msg.offset
}

func (msg *MessageView) SetKeys(keys ...string) {
	msg.keys = keys
}

func (msg *MessageView) SetDelayTimeLevel(deliveryTimestamp time.Time) {
	msg.deliveryTimestamp = &deliveryTimestamp
}

func (msg *MessageView) SetMessageGroup(messageGroup string) {
	msg.messageGroup = &messageGroup
}
