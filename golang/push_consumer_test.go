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
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

func TestDefaultPushConsumer_WrapReceiveMessageRequest(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	pc, err := newPushConsumer(config,
		WithPushSubscriptionExpressions(map[string]*FilterExpression{"test-topic": NewFilterExpressionWithType("TagA", TAG)}),
		WithPushMessageListener(&FuncMessageListener{Consume: func(*MessageView) ConsumerResult { return SUCCESS }}),
	)
	if err != nil {
		t.Fatalf("failed to create push consumer: %v", err)
	}

	messageQueue := &v2.MessageQueue{
		Topic: &v2.Resource{
			Name:              "test-topic",
			ResourceNamespace: "test-namespace",
		},
		Id: 1,
		Broker: &v2.Broker{
			Name:      "test-broker",
			Endpoints: fakeEndpoints(),
		},
	}
	longPollingTimeout := 30 * time.Second
	batchSize := 32

	// TAG 类型
	req := pc.WrapReceiveMessageRequest(batchSize, messageQueue, NewFilterExpressionWithType("TagA", TAG), longPollingTimeout)
	if req.GetGroup().GetName() != "test-group" {
		t.Errorf("expected group name 'test-group', got %s", req.GetGroup().GetName())
	}
	if req.GetFilterExpression().GetType() != v2.FilterType_TAG {
		t.Errorf("expected filter type TAG, got %v", req.GetFilterExpression().GetType())
	}
	if req.GetFilterExpression().GetExpression() != "TagA" {
		t.Errorf("expected expression 'TagA', got %s", req.GetFilterExpression().GetExpression())
	}
	if req.GetBatchSize() != int32(batchSize) {
		t.Errorf("expected batch size %d, got %d", batchSize, req.GetBatchSize())
	}

	// 未指定类型
	req2 := pc.WrapReceiveMessageRequest(1, messageQueue, &FilterExpression{expressionType: 999, expression: "x"}, longPollingTimeout)
	if req2.GetFilterExpression().GetType() != v2.FilterType_FILTER_TYPE_UNSPECIFIED {
		t.Errorf("expected filter type UNSPECIFIED, got %v", req2.GetFilterExpression().GetType())
	}
}

func TestDefaultPushConsumer_WrapHeartbeatRequest(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	pc, err := newPushConsumer(config,
		WithPushSubscriptionExpressions(map[string]*FilterExpression{"test-topic": NewFilterExpression("*")}),
		WithPushMessageListener(&FuncMessageListener{Consume: func(*MessageView) ConsumerResult { return SUCCESS }}),
	)
	if err != nil {
		t.Fatalf("failed to create push consumer: %v", err)
	}

	req := pc.WrapHeartbeatRequest()
	if req.GetGroup().GetName() != "test-group" {
		t.Errorf("expected group name 'test-group', got %s", req.GetGroup().GetName())
	}
	if req.GetClientType() != v2.ClientType_PUSH_CONSUMER {
		t.Errorf("expected client type PUSH_CONSUMER, got %v", req.GetClientType())
	}
}

func TestDefaultPushConsumer_wrapAckMessageRequest(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	pc, err := newPushConsumer(config,
		WithPushSubscriptionExpressions(map[string]*FilterExpression{"test-topic": NewFilterExpression("*")}),
		WithPushMessageListener(&FuncMessageListener{Consume: func(*MessageView) ConsumerResult { return SUCCESS }}),
	)
	if err != nil {
		t.Fatalf("failed to create push consumer: %v", err)
	}

	// 普通消息
	mv := &MessageView{
		messageId:     "msg-123",
		topic:         "test-topic",
		ReceiptHandle: "receipt-123",
	}
	req := pc.wrapAckMessageRequest(mv)
	if req.GetGroup().GetName() != pc.pcSettings.groupName.GetName() {
		t.Errorf("expected group name %s, got %s", pc.pcSettings.groupName.GetName(), req.GetGroup().GetName())
	}
	if req.GetTopic().GetName() != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %s", req.GetTopic().GetName())
	}
	if len(req.GetEntries()) != 1 {
		t.Errorf("expected 1 entry, got %d", len(req.GetEntries()))
	}
	entry := req.GetEntries()[0]
	if entry.GetMessageId() != "msg-123" {
		t.Errorf("expected message id 'msg-123', got %s", entry.GetMessageId())
	}
	if entry.GetReceiptHandle() != "receipt-123" {
		t.Errorf("expected receipt handle 'receipt-123', got %s", entry.GetReceiptHandle())
	}
	if entry.GetLiteTopic() != "" {
		t.Error("expected lite topic to be nil for normal message")
	}

	// Lite 消息
	mvLite := &MessageView{
		messageId:     "msg-456",
		topic:         "test-topic",
		ReceiptHandle: "receipt-456",
		liteTopic:     "lite-topic-123",
	}
	req2 := pc.wrapAckMessageRequest(mvLite)
	entry2 := req2.GetEntries()[0]
	if entry2.GetLiteTopic() != "" {
		t.Error("expected lite topic to be nil for normal message")
	}
}

func TestDefaultPushConsumer_wrapAckMessageRequest_lite(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	pc, err := newPushConsumer(config,
		WithPushSubscriptionExpressions(map[string]*FilterExpression{"test-topic": NewFilterExpression("*")}),
		WithPushMessageListener(&FuncMessageListener{Consume: func(*MessageView) ConsumerResult { return SUCCESS }}),
	)
	if err != nil {
		t.Fatalf("failed to create push consumer: %v", err)
	}
	// LitePushConsumer for test
	pc.pcSettings.clientType = v2.ClientType_LITE_PUSH_CONSUMER

	// 普通消息
	mv := &MessageView{
		messageId:     "msg-123",
		topic:         "test-topic",
		ReceiptHandle: "receipt-123",
	}
	req := pc.wrapAckMessageRequest(mv)
	if req.GetGroup().GetName() != pc.pcSettings.groupName.GetName() {
		t.Errorf("expected group name %s, got %s", pc.pcSettings.groupName.GetName(), req.GetGroup().GetName())
	}
	if req.GetTopic().GetName() != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %s", req.GetTopic().GetName())
	}
	if len(req.GetEntries()) != 1 {
		t.Errorf("expected 1 entry, got %d", len(req.GetEntries()))
	}
	entry := req.GetEntries()[0]
	if entry.GetMessageId() != "msg-123" {
		t.Errorf("expected message id 'msg-123', got %s", entry.GetMessageId())
	}
	if entry.GetReceiptHandle() != "receipt-123" {
		t.Errorf("expected receipt handle 'receipt-123', got %s", entry.GetReceiptHandle())
	}
	if entry.GetLiteTopic() != "" {
		t.Error("expected lite topic to be nil for normal message")
	}

	// Lite 消息
	mvLite := &MessageView{
		messageId:     "msg-456",
		topic:         "test-topic",
		ReceiptHandle: "receipt-456",
		liteTopic:     "lite-topic-123",
	}
	req2 := pc.wrapAckMessageRequest(mvLite)
	entry2 := req2.GetEntries()[0]
	if entry2.GetLiteTopic() == "" {
		t.Error("expected lite topic to be set")
	} else if entry2.GetLiteTopic() != "lite-topic-123" {
		t.Errorf("expected lite topic '%s', got %s", "lite-topic-123", entry2.GetLiteTopic())
	}
}

func TestDefaultLitePushConsumer_Wraps(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	lpc, err := NewLitePushConsumer(config, &LitePushConsumerConfig{bindTopic: "bind-topic"}, WithPushMessageListener(&FuncMessageListener{Consume: func(*MessageView) ConsumerResult { return SUCCESS }}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	// 类型断言为具体类型以调用 Wrap* 方法（接口 LitePushConsumer 上未暴露这些方法）
	dlpc, ok := lpc.(*defaultLitePushConsumer)
	if !ok {
		t.Fatalf("failed to assert lite push consumer concrete type")
	}

	messageQueue := &v2.MessageQueue{
		Topic: &v2.Resource{
			Name:              "bind-topic",
			ResourceNamespace: "test-namespace",
		},
		Id: 1,
		Broker: &v2.Broker{
			Name:      "test-broker",
			Endpoints: fakeEndpoints(),
		},
	}

	req := dlpc.WrapReceiveMessageRequest(5, messageQueue, SUB_ALL, time.Second*10)
	if req.GetGroup().GetName() != "test-group" {
		t.Errorf("expected group name 'test-group', got %s", req.GetGroup().GetName())
	}
	if req.GetAutoRenew() {
		t.Error("expected auto renew to be true")
	}

	hb := dlpc.WrapHeartbeatRequest()
	if hb.GetClientType() != v2.ClientType_LITE_PUSH_CONSUMER {
		t.Errorf("expected client type LITE_PUSH_CONSUMER, got %v", hb.GetClientType())
	}
}
