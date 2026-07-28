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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
)

func createTestLiteSimpleConsumer(t *testing.T) (*defaultLiteSimpleConsumer, error) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LiteSimpleConsumerConfig{bindTopic: "bind-topic"}

	lsc, err := NewLiteSimpleConsumer(config, liteConfig, WithSimpleAwaitDuration(time.Second*5))
	if err != nil {
		return nil, err
	}

	dlsc := lsc.(*defaultLiteSimpleConsumer)

	dlsc.cli.on.Store(true)

	mockedClientManager := &mockedClientManager{
		mockRpcClient: mockRpcClient,
	}

	dlsc.cli.clientManager = mockedClientManager

	if dlsc.cli != dlsc.defaultSimpleConsumer.cli {
		t.Errorf("Expected dlsc.cli and dlsc.defaultSimpleConsumer.cli to be the same instance")
	}

	return dlsc, nil
}

func TestNewLiteSimpleConsumer(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}

	liteConfig := NewLiteSimpleConsumerConfig("bind-topic")
	lsc, err := NewLiteSimpleConsumer(config, liteConfig)
	if err != nil {
		t.Fatalf("failed to create lite simple consumer: %v", err)
	}

	dlsc := lsc.(*defaultLiteSimpleConsumer)
	if dlsc.liteSimpleConsumerSettings.bindTopic != "bind-topic" {
		t.Errorf("expected bind topic 'bind-topic', got %s", dlsc.liteSimpleConsumerSettings.bindTopic)
	}

	if int32(dlsc.scSettings.clientType) != int32(v2.ClientType_LITE_SIMPLE_CONSUMER) {
		t.Errorf("expected client type LITE_SIMPLE_CONSUMER, got %v", dlsc.scSettings.clientType)
	}

	// Default subscription: (bindTopic, *) for code reuse.
	fe, ok := (*dlsc.subscriptionExpressions)["bind-topic"]
	if !ok {
		t.Fatal("expected default subscription for bind topic")
	}
	if fe.expression != SUB_ALL.expression || fe.expressionType != SUB_ALL.expressionType {
		t.Errorf("expected default subscription SUB_ALL, got %+v", fe)
	}

	if _, ok := dlsc.cli.settings.(*liteSimpleConsumerSettings); !ok {
		t.Errorf("expected client settings to be liteSimpleConsumerSettings, got %T", dlsc.cli.settings)
	}

	if dlsc.liteSimpleConsumerSettings.liteSubscriptionManager != dlsc.liteSubscriptionManager {
		t.Error("expected settings and consumer to share the same liteSubscriptionManager")
	}
}

func TestNewLiteSimpleConsumer_NilLiteConfig(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}

	_, err := NewLiteSimpleConsumer(config, nil)
	if err == nil {
		t.Fatal("expected error for nil lite config, got nil")
	}

	expectedError := "LiteSimpleConsumerConfig is required"
	if err.Error() != expectedError {
		t.Errorf("expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestNewLiteSimpleConsumer_EmptyBindTopic(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}

	liteConfig := &LiteSimpleConsumerConfig{bindTopic: ""}
	_, err := NewLiteSimpleConsumer(config, liteConfig)
	if err == nil {
		t.Fatal("expected error for empty bind topic, got nil")
	}

	expectedError := "LiteSimpleConsumerConfig.bindTopic is required"
	if err.Error() != expectedError {
		t.Errorf("expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestLiteSimpleConsumer_SubscribeLite(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
		if req.GetAction() != v2.LiteSubscriptionAction_PARTIAL_ADD {
			t.Errorf("expected action PARTIAL_ADD, got %v", req.GetAction())
		}
		if req.GetTopic().GetName() != "bind-topic" {
			t.Errorf("expected topic 'bind-topic', got %s", req.GetTopic().GetName())
		}
		if req.GetGroup().GetName() != "test-group" {
			t.Errorf("expected group 'test-group', got %s", req.GetGroup().GetName())
		}
		if len(req.GetLiteTopicSet()) != 1 || req.GetLiteTopicSet()[0] != "lite-topic-1" {
			t.Errorf("expected lite topic set ['lite-topic-1'], got %v", req.GetLiteTopicSet())
		}
		return setupSuccessResponse(), nil
	}).Times(1)

	err = dlsc.SubscribeLite("lite-topic-1")
	if err != nil {
		t.Fatalf("expected no error for SubscribeLite, got %v", err)
	}

	if _, exists := dlsc.liteSubscriptionManager.liteTopicSet.Load("lite-topic-1"); !exists {
		t.Error("expected lite topic to be added to set")
	}
}

func TestLiteSimpleConsumer_SubscribeLite_WithOffset(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	offsetOption, err := NewOffsetOptionWithOffset(100)
	if err != nil {
		t.Fatalf("failed to create offset option: %v", err)
	}

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
		if req.GetOffsetOption() == nil {
			t.Fatal("expected offset option to be set")
		}
		if req.GetOffsetOption().GetOffset() != 100 {
			t.Errorf("expected offset 100, got %d", req.GetOffsetOption().GetOffset())
		}
		return setupSuccessResponse(), nil
	}).Times(1)

	err = dlsc.SubscribeLite("lite-topic-1", offsetOption)
	if err != nil {
		t.Fatalf("expected no error for SubscribeLite with offset, got %v", err)
	}
}

func TestLiteSimpleConsumer_SubscribeLite_NotRunning(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	dlsc.cli.on.Store(false)

	err = dlsc.SubscribeLite("lite-topic-1")
	if err == nil {
		t.Fatal("expected error when consumer not running")
	}

	expectedErrorPrefix := "client not running, clientId="
	if !strings.HasPrefix(err.Error(), expectedErrorPrefix) {
		t.Errorf("expected error prefix '%s', got '%s'", expectedErrorPrefix, err.Error())
	}
}

func TestLiteSimpleConsumer_SubscribeLite_RpcError(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(nil, errors.New("rpc error"))

	err = dlsc.SubscribeLite("lite-topic-1")
	if err == nil {
		t.Fatal("expected rpc error")
	}

	if _, exists := dlsc.liteSubscriptionManager.liteTopicSet.Load("lite-topic-1"); exists {
		t.Error("lite topic should not be added when rpc fails")
	}
}

func TestLiteSimpleConsumer_UnSubscribeLite(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	dlsc.liteSubscriptionManager.liteTopicSet.Store("lite-topic-1", struct{}{})

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
		if req.GetAction() != v2.LiteSubscriptionAction_PARTIAL_REMOVE {
			t.Errorf("expected action PARTIAL_REMOVE, got %v", req.GetAction())
		}
		if len(req.GetLiteTopicSet()) != 1 || req.GetLiteTopicSet()[0] != "lite-topic-1" {
			t.Errorf("expected lite topic set ['lite-topic-1'], got %v", req.GetLiteTopicSet())
		}
		return setupSuccessResponse(), nil
	})

	err = dlsc.UnSubscribeLite("lite-topic-1")
	if err != nil {
		t.Fatalf("expected no error for UnSubscribeLite, got %v", err)
	}

	if _, exists := dlsc.liteSubscriptionManager.liteTopicSet.Load("lite-topic-1"); exists {
		t.Error("expected lite topic to be removed from set")
	}
}

func TestLiteSimpleConsumer_GetLiteTopicSet(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	dlsc.liteSubscriptionManager.liteTopicSet.Store("lite-topic-1", struct{}{})
	dlsc.liteSubscriptionManager.liteTopicSet.Store("lite-topic-2", struct{}{})

	topics := dlsc.GetLiteTopicSet()
	if len(topics) != 2 {
		t.Errorf("expected 2 lite topics, got %d", len(topics))
	}
}

func TestLiteSimpleConsumer_notifyUnsubscribeLite(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	dlsc.liteSubscriptionManager.liteTopicSet.Store("lite-topic-notify", struct{}{})

	cmd := &v2.NotifyUnsubscribeLiteCommand{
		LiteTopic: "lite-topic-notify",
	}

	dlsc.liteSubscriptionManager.onNotifyUnsubscribeLiteCommand(cmd)

	if _, exists := dlsc.liteSubscriptionManager.liteTopicSet.Load("lite-topic-notify"); exists {
		t.Error("expected lite topic to be removed from set after notify")
	}
}

func TestLiteSimpleConsumer_wrapHeartbeatRequest(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	req := dlsc.wrapHeartbeatRequest()

	if req.GetGroup().GetName() != "test-group" {
		t.Errorf("expected group name 'test-group', got %s", req.GetGroup().GetName())
	}

	if req.GetGroup().GetResourceNamespace() != "test-namespace" {
		t.Errorf("expected namespace 'test-namespace', got %s", req.GetGroup().GetResourceNamespace())
	}

	if int32(req.GetClientType()) != int32(v2.ClientType_LITE_SIMPLE_CONSUMER) {
		t.Errorf("expected client type LITE_SIMPLE_CONSUMER, got %v", req.GetClientType())
	}
}

func TestLiteSimpleConsumerSettings_applySettingsCommand(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	settings := dlsc.liteSimpleConsumerSettings

	liteQuota := int32(100)
	maxSize := int32(1024)

	testSettings := &v2.Settings{
		PubSub: &v2.Settings_Subscription{
			Subscription: &v2.Subscription{
				LiteSubscriptionQuota: &liteQuota,
				MaxLiteTopicSize:      &maxSize,
			},
		},
	}

	err = settings.applySettingsCommand(testSettings)
	if err != nil {
		t.Fatalf("applySettingsCommand failed: %v", err)
	}

	if dlsc.liteSubscriptionManager.liteSubscriptionQuota != liteQuota {
		t.Errorf("expected lite subscription quota %d, got %d", liteQuota, dlsc.liteSubscriptionManager.liteSubscriptionQuota)
	}

	if dlsc.liteSubscriptionManager.maxLiteTopicSize != maxSize {
		t.Errorf("expected max lite topic size %d, got %d", maxSize, dlsc.liteSubscriptionManager.maxLiteTopicSize)
	}
}

func TestLiteSimpleConsumerSettings_applySettingsCommand_InvalidPubSub(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	err = dlsc.liteSimpleConsumerSettings.applySettingsCommand(&v2.Settings{})
	if err == nil {
		t.Fatal("expected error for settings without subscription pubsub")
	}
}

func TestLiteSimpleConsumerSettings_toProtobuf(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	protobuf := dlsc.liteSimpleConsumerSettings.toProtobuf()

	if int32(protobuf.GetClientType()) != int32(v2.ClientType_LITE_SIMPLE_CONSUMER) {
		t.Errorf("expected client type LITE_SIMPLE_CONSUMER, got %v", protobuf.GetClientType())
	}

	subscription, ok := protobuf.GetPubSub().(*v2.Settings_Subscription)
	if !ok || subscription == nil {
		t.Fatal("expected subscription to be set")
	}

	// quota and maxLiteTopicSize are server-pushed only, never reported back
	if subscription.Subscription.LiteSubscriptionQuota != nil {
		t.Errorf("expected lite subscription quota to be unset, got %d", subscription.Subscription.GetLiteSubscriptionQuota())
	}

	if subscription.Subscription.MaxLiteTopicSize != nil {
		t.Errorf("expected max lite topic size to be unset, got %d", subscription.Subscription.GetMaxLiteTopicSize())
	}

	entry := subscription.Subscription.GetSubscriptions()[0]

	if entry.GetTopic().GetName() != "bind-topic" {
		t.Errorf("expected topic name 'bind-topic', got %s", entry.GetTopic().GetName())
	}

	if entry.GetTopic().GetResourceNamespace() != "test-namespace" {
		t.Errorf("expected namespace 'test-namespace', got %s", entry.GetTopic().GetResourceNamespace())
	}
}

func TestLiteSimpleConsumerSettings_Getters(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	settings := dlsc.liteSimpleConsumerSettings

	if settings.GetClientID() != dlsc.cli.GetClientID() {
		t.Errorf("expected client id %s, got %s", dlsc.cli.GetClientID(), settings.GetClientID())
	}
	if int32(settings.GetClientType()) != int32(v2.ClientType_LITE_SIMPLE_CONSUMER) {
		t.Errorf("expected client type LITE_SIMPLE_CONSUMER, got %v", settings.GetClientType())
	}
	if settings.GetAccessPoint() != dlsc.scSettings.endpoints {
		t.Error("expected access point to delegate to simple consumer settings")
	}
	if settings.GetRequestTimeout() != dlsc.scSettings.requestTimeout {
		t.Errorf("expected request timeout %v, got %v", dlsc.scSettings.requestTimeout, settings.GetRequestTimeout())
	}
	if settings.GetRetryPolicy() != dlsc.scSettings.retryPolicy {
		t.Error("expected retry policy to delegate to simple consumer settings")
	}
}

func TestLiteSimpleConsumer_wrapAckMessageRequest_WithLiteTopic(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	mv := &MessageView{
		messageId:     "msg-1",
		topic:         "bind-topic",
		liteTopic:     "lite-topic-1",
		ReceiptHandle: "handle-1",
	}

	request := dlsc.wrapAckMessageRequest(mv)

	entry := request.GetEntries()[0]
	if entry.GetLiteTopic() != "lite-topic-1" {
		t.Errorf("expected lite topic 'lite-topic-1' in ack entry, got %s", entry.GetLiteTopic())
	}
	if entry.GetMessageId() != "msg-1" {
		t.Errorf("expected message id 'msg-1', got %s", entry.GetMessageId())
	}
	if entry.GetReceiptHandle() != "handle-1" {
		t.Errorf("expected receipt handle 'handle-1', got %s", entry.GetReceiptHandle())
	}
}

func TestSimpleConsumer_wrapAckMessageRequest_NoLiteTopic(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	sc, err := newSimpleConsumer(config)
	if err != nil {
		t.Fatalf("failed to create simple consumer: %v", err)
	}

	mv := &MessageView{
		messageId:     "msg-1",
		topic:         "normal-topic",
		liteTopic:     "lite-topic-1",
		ReceiptHandle: "handle-1",
	}

	request := sc.wrapAckMessageRequest(mv)

	// normal simple consumer never carries lite topic even if the message has one
	if request.GetEntries()[0].LiteTopic != nil {
		t.Errorf("expected lite topic to be unset for simple consumer, got %s", request.GetEntries()[0].GetLiteTopic())
	}
}

type captureChangeInvisibleClientManager struct {
	*mockedClientManager
	capturedRequest *v2.ChangeInvisibleDurationRequest
}

func (m *captureChangeInvisibleClientManager) ChangeInvisibleDuration(ctx context.Context, endpoints *v2.Endpoints, request *v2.ChangeInvisibleDurationRequest, duration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	m.capturedRequest = request
	return &v2.ChangeInvisibleDurationResponse{
		Status:        &v2.Status{Code: v2.Code_OK},
		ReceiptHandle: "new-handle",
	}, nil
}

func TestLiteSimpleConsumer_ChangeInvisibleDuration_WithLiteTopic(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	captureManager := &captureChangeInvisibleClientManager{
		mockedClientManager: &mockedClientManager{mockRpcClient: mockRpcClient},
	}
	dlsc.cli.clientManager = captureManager

	mv := &MessageView{
		messageId:     "msg-1",
		topic:         "bind-topic",
		liteTopic:     "lite-topic-1",
		ReceiptHandle: "handle-1",
		endpoints:     &v2.Endpoints{Scheme: v2.AddressScheme_IPv4},
	}

	err = dlsc.ChangeInvisibleDuration(mv, time.Second*30)
	if err != nil {
		t.Fatalf("expected no error for ChangeInvisibleDuration, got %v", err)
	}

	request := captureManager.capturedRequest
	if request == nil {
		t.Fatal("expected ChangeInvisibleDuration request to be captured")
	}
	if request.GetLiteTopic() != "lite-topic-1" {
		t.Errorf("expected lite topic 'lite-topic-1', got %s", request.GetLiteTopic())
	}
	if !request.GetSuspend() {
		t.Error("expected suspend to be true for lite consumer")
	}
	if mv.ReceiptHandle != "new-handle" {
		t.Errorf("expected receipt handle updated to 'new-handle', got %s", mv.ReceiptHandle)
	}
}

func buildTestMessageQueue(brokerId int32, permission v2.Permission) *v2.MessageQueue {
	return &v2.MessageQueue{
		Permission: permission,
		Broker: &v2.Broker{
			Name: "broker-test",
			Id:   brokerId,
		},
	}
}

func TestLiteSimpleConsumer_filterTopicRouteData(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlsc, err := createTestLiteSimpleConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite simple consumer: %v", err)
	}

	writeOnlyMaster := buildTestMessageQueue(0, v2.Permission_WRITE)
	readableSlave := buildTestMessageQueue(1, v2.Permission_READ_WRITE)
	readableMaster := buildTestMessageQueue(0, v2.Permission_READ)
	anotherReadableMaster := buildTestMessageQueue(0, v2.Permission_READ_WRITE)

	queues := []*v2.MessageQueue{writeOnlyMaster, readableSlave, readableMaster, anotherReadableMaster}

	// lite consumer keeps only the first readable master queue
	filtered := dlsc.filterTopicRouteData(queues)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 queue after filtering, got %d", len(filtered))
	}
	if filtered[0] != readableMaster {
		t.Error("expected the first readable master queue to be kept")
	}

	// no readable master queue results in an empty route
	filtered = dlsc.filterTopicRouteData([]*v2.MessageQueue{writeOnlyMaster, readableSlave})
	if len(filtered) != 0 {
		t.Errorf("expected empty route when no readable master queue, got %d", len(filtered))
	}
}

func TestSimpleConsumer_filterTopicRouteData_NoFiltering(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	sc, err := newSimpleConsumer(config)
	if err != nil {
		t.Fatalf("failed to create simple consumer: %v", err)
	}

	queues := []*v2.MessageQueue{
		buildTestMessageQueue(0, v2.Permission_WRITE),
		buildTestMessageQueue(1, v2.Permission_READ_WRITE),
	}

	// normal simple consumer keeps the route untouched
	filtered := sc.filterTopicRouteData(queues)
	if len(filtered) != len(queues) {
		t.Errorf("expected %d queues without filtering, got %d", len(queues), len(filtered))
	}
}

func TestIsReadableMasterQueue(t *testing.T) {
	cases := []struct {
		name     string
		queue    *v2.MessageQueue
		expected bool
	}{
		{"read master", buildTestMessageQueue(0, v2.Permission_READ), true},
		{"read-write master", buildTestMessageQueue(0, v2.Permission_READ_WRITE), true},
		{"write-only master", buildTestMessageQueue(0, v2.Permission_WRITE), false},
		{"readable slave", buildTestMessageQueue(1, v2.Permission_READ_WRITE), false},
		{"none master", buildTestMessageQueue(0, v2.Permission_NONE), false},
	}

	for _, c := range cases {
		if got := isReadableMasterQueue(c.queue); got != c.expected {
			t.Errorf("case %s: expected %v, got %v", c.name, c.expected, got)
		}
	}
}
