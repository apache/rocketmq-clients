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
	"fmt"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	mockCtrl      *gomock.Controller
	mockRpcClient *MockRpcClient
)

func setupTest(t *testing.T) {
	mockCtrl = gomock.NewController(t)
	mockRpcClient = NewMockRpcClient(mockCtrl)
}

func teardownTest() {
	mockCtrl.Finish()
}

func setupSuccessResponse() *v2.SyncLiteSubscriptionResponse {
	return &v2.SyncLiteSubscriptionResponse{
		Status: &v2.Status{Code: v2.Code_OK},
	}
}

func setupErrorResponse(code v2.Code, message string) *v2.SyncLiteSubscriptionResponse {
	return &v2.SyncLiteSubscriptionResponse{
		Status: &v2.Status{
			Code:    code,
			Message: message,
		},
	}
}

func TestNewLitePushConsumer(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}

	liteConfig := &LitePushConsumerConfig{bindTopic: "bind-topic"}
	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)
	if dlpc.litePushConsumerSettings.bindTopic != "bind-topic" {
		t.Errorf("expected bind topic 'bind-topic', got %s", dlpc.litePushConsumerSettings.bindTopic)
	}

	if int32(dlpc.pcSettings.clientType) != int32(v2.ClientType_LITE_PUSH_CONSUMER) {
		t.Errorf("expected client type LITE_PUSH_CONSUMER, got %v", dlpc.pcSettings.clientType)
	}

	if !dlpc.pcSettings.isFifo {
		t.Error("expected isFifo to be true for lite push consumer")
	}
}

func TestNewLitePushConsumer_EmptyBindTopic(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}

	liteConfig := &LitePushConsumerConfig{bindTopic: ""}
	_, err := NewLitePushConsumer(config, liteConfig)
	if err == nil {
		t.Fatal("expected error for empty bind topic, got nil")
	}

	expectedError := "LitePushConsumerConfig.bindTopic is required"
	if err.Error() != expectedError {
		t.Errorf("expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func createTestLitePushConsumer(t *testing.T) (*defaultLitePushConsumer, error) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{bindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		return nil, err
	}

	dlpc := lpc.(*defaultLitePushConsumer)

	dlpc.cli.on.Store(true)

	mockedClientManager := &mockedClientManager{
		mockRpcClient: mockRpcClient,
	}

	dlpc.cli.clientManager = mockedClientManager

	if dlpc.cli != dlpc.defaultPushConsumer.cli {
		t.Errorf("Expected dlpc.cli and dlpc.defaultPushConsumer.cli to be the same instance")
	}

	return dlpc, nil
}

type mockedClientManager struct {
	mockRpcClient *MockRpcClient
}

func (m *mockedClientManager) RegisterClient(client Client)   {}
func (m *mockedClientManager) UnRegisterClient(client Client) {}
func (m *mockedClientManager) QueryRoute(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryRouteRequest, duration time.Duration) (*v2.QueryRouteResponse, error) {
	return nil, nil
}
func (m *mockedClientManager) QueryAssignments(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryAssignmentRequest, duration time.Duration) (*v2.QueryAssignmentResponse, error) {
	return nil, nil
}
func (m *mockedClientManager) HeartBeat(ctx context.Context, endpoints *v2.Endpoints, request *v2.HeartbeatRequest, duration time.Duration) (*v2.HeartbeatResponse, error) {
	return nil, nil
}
func (m *mockedClientManager) SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error) {
	return nil, nil
}
func (m *mockedClientManager) Telemetry(ctx context.Context, endpoints *v2.Endpoints, duration time.Duration) (v2.MessagingService_TelemetryClient, error) {
	return nil, nil
}
func (m *mockedClientManager) EndTransaction(ctx context.Context, endpoints *v2.Endpoints, request *v2.EndTransactionRequest, duration time.Duration) (*v2.EndTransactionResponse, error) {
	return nil, nil
}
func (m *mockedClientManager) NotifyClientTermination(ctx context.Context, endpoints *v2.Endpoints, request *v2.NotifyClientTerminationRequest, duration time.Duration) (*v2.NotifyClientTerminationResponse, error) {
	return nil, nil
}
func (m *mockedClientManager) ReceiveMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	return nil, nil
}
func (m *mockedClientManager) AckMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.AckMessageRequest, duration time.Duration) (*v2.AckMessageResponse, error) {
	return nil, nil
}
func (m *mockedClientManager) ChangeInvisibleDuration(ctx context.Context, endpoints *v2.Endpoints, request *v2.ChangeInvisibleDurationRequest, duration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	return nil, nil
}
func (m *mockedClientManager) ForwardMessageToDeadLetterQueue(ctx context.Context, endpoints *v2.Endpoints, request *v2.ForwardMessageToDeadLetterQueueRequest, duration time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
	return nil, nil
}

func (m *mockedClientManager) SyncLiteSubscription(ctx context.Context, endpoints *v2.Endpoints, request *v2.SyncLiteSubscriptionRequest, duration time.Duration) (*v2.SyncLiteSubscriptionResponse, error) {
	fmt.Printf("DEBUG: mockedClientManager.SyncLiteSubscription called with request: %+v\n", request)
	return m.mockRpcClient.SyncLiteSubscription(ctx, request)
}

func TestLitePushConsumer_SubscribeLite(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	if dlpc.defaultPushConsumer.cli.clientManager == nil {
		t.Fatal("clientManager should not be nil")
	}

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(setupSuccessResponse(), nil).Times(1)

	err = dlpc.SubscribeLite("lite-topic-1")
	if err != nil {
		t.Fatalf("expected no error for SubscribeLite, got %v", err)
	}

	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet.Load("lite-topic-1"); !exists {
		t.Error("expected lite topic to be added to set")
	}
}

func TestLitePushConsumer_SubscribeLite_NotRunning(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	dlpc.cli.on.Store(false)

	err = dlpc.SubscribeLite("lite-topic-1")
	if err == nil {
		t.Fatal("expected error when consumer not running")
	}

	expectedError := "consumer is not running"
	if err.Error() != expectedError {
		t.Errorf("expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestLitePushConsumer_SubscribeLite_RpcError(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(nil, errors.New("rpc error"))

	err = dlpc.SubscribeLite("lite-topic-1")
	if err == nil {
		t.Fatal("expected rpc error")
	}

	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet.Load("lite-topic-1"); exists {
		t.Error("lite topic should not be added when rpc fails")
	}
}

func TestLitePushConsumer_UnSubscribeLite(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	dlpc.litePushConsumerSettings.liteTopicSet.Store("lite-topic-1", struct{}{})

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
		if req.Action != v2.LiteSubscriptionAction_PARTIAL_REMOVE {
			t.Errorf("expected action INCREMENTAL_REMOVE, got %v", req.Action)
		}
		if len(req.LiteTopicSet) != 1 || req.LiteTopicSet[0] != "lite-topic-1" {
			t.Errorf("expected lite topic set ['lite-topic-1'], got %v", req.LiteTopicSet)
		}
		return setupSuccessResponse(), nil
	})

	err = dlpc.UnSubscribeLite("lite-topic-1")
	if err != nil {
		t.Fatalf("expected no error for UnSubscribeLite, got %v", err)
	}

	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet.Load("lite-topic-1"); exists {
		t.Error("expected lite topic to be removed from set")
	}
}

func TestLitePushConsumer_notifyUnsubscribeLite(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	dlpc.litePushConsumerSettings.liteTopicSet.Store("lite-topic-notify", struct{}{})

	cmd := &v2.NotifyUnsubscribeLiteCommand{
		LiteTopic: "lite-topic-notify",
	}

	dlpc.notifyUnsubscribeLite(cmd)

	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet.Load("lite-topic-notify"); exists {
		t.Error("expected lite topic to be removed from set after notify")
	}
}

func TestLitePushConsumer_notifyUnsubscribeLite_EmptyLiteTopic(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	dlpc.litePushConsumerSettings.liteTopicSet.Store("lite-topic-keep", struct{}{})

	cmd := &v2.NotifyUnsubscribeLiteCommand{
		LiteTopic: "",
	}

	dlpc.notifyUnsubscribeLite(cmd)

	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet.Load("lite-topic-keep"); !exists {
		t.Error("lite topic should not be removed when command has empty lite topic")
	}
}

func TestLitePushConsumer_syncLiteSubscription_StatusError(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(setupErrorResponse(v2.Code_INTERNAL_SERVER_ERROR, "internal error"), nil)

	err = dlpc.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_PARTIAL_ADD, []string{"test"})
	if err == nil {
		t.Fatal("expected error for non-OK status code")
	}

	rpcErr, ok := err.(*ErrRpcStatus)
	if !ok {
		t.Fatalf("expected ErrRpcStatus, got %T", err)
	}

	if rpcErr.Code != int32(v2.Code_INTERNAL_SERVER_ERROR) {
		t.Errorf("expected code %d, got %d", v2.Code_INTERNAL_SERVER_ERROR, rpcErr.Code)
	}

	if rpcErr.Message != "internal error" {
		t.Errorf("expected message 'internal error', got '%s'", rpcErr.Message)
	}
}

func TestLitePushConsumer_WrapReceiveMessageRequest(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
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

	if req.GetGroup().GetResourceNamespace() != "test-namespace" {
		t.Errorf("expected namespace 'test-namespace', got %s", req.GetGroup().GetResourceNamespace())
	}

	if req.GetBatchSize() != 5 {
		t.Errorf("expected batch size 5, got %d", req.GetBatchSize())
	}

	if req.GetAutoRenew() {
		t.Error("expected auto renew to be true for lite push consumer")
	}

	if req.GetAttemptId() == "" {
		t.Error("expected attempt id to be set")
	}

	if req.GetLongPollingTimeout().GetSeconds() != int64(10) {
		t.Errorf("expected polling timeout 10s, got %v", req.GetLongPollingTimeout().GetSeconds())
	}
}

func TestLitePushConsumer_WrapHeartbeatRequest(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	req := dlpc.WrapHeartbeatRequest()

	if req.GetGroup().GetName() != "test-group" {
		t.Errorf("expected group name 'test-group', got %s", req.GetGroup().GetName())
	}

	if req.GetGroup().GetResourceNamespace() != "test-namespace" {
		t.Errorf("expected namespace 'test-namespace', got %s", req.GetGroup().GetResourceNamespace())
	}

	if int32(req.GetClientType()) != int32(v2.ClientType_LITE_PUSH_CONSUMER) {
		t.Errorf("expected client type LITE_PUSH_CONSUMER, got %v", req.GetClientType())
	}
}

func TestLitePushConsumerSettings_applySettingsCommand(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	settings := dlpc.litePushConsumerSettings

	liteQuota := int32(100)
	maxSize := int32(1024)
	fifoVal := false

	testSettings := &v2.Settings{
		PubSub: &v2.Settings_Subscription{
			Subscription: &v2.Subscription{
				LiteSubscriptionQuota: &liteQuota,
				MaxLiteTopicSize:      &maxSize,
				Fifo:                  &fifoVal,
			},
		},
		BackoffPolicy: &v2.RetryPolicy{
			MaxAttempts: 3,
			Strategy: &v2.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &v2.ExponentialBackoff{
					Initial:    durationpb.New(time.Second),
					Max:        durationpb.New(60 * time.Second),
					Multiplier: 2.0,
				},
			},
		},
	}

	err = settings.applySettingsCommand(testSettings)
	if err != nil {
		t.Fatalf("applySettingsCommand failed: %v", err)
	}

	if settings.liteSubscriptionQuota != liteQuota {
		t.Errorf("expected lite subscription quota %d, got %d", liteQuota, settings.liteSubscriptionQuota)
	}

	if settings.maxLiteTopicSize != maxSize {
		t.Errorf("expected max lite topic size %d, got %d", maxSize, settings.maxLiteTopicSize)
	}

	if !settings.isFifo {
		t.Error("expected isFifo to be forced to true")
	}
}

func TestLitePushConsumerSettings_toProtobuf(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	settings := dlpc.litePushConsumerSettings

	settings.liteSubscriptionQuota = 50
	settings.maxLiteTopicSize = 512

	protobuf := settings.toProtobuf()

	if int32(protobuf.GetClientType()) != int32(v2.ClientType_LITE_PUSH_CONSUMER) {
		t.Errorf("expected client type LITE_PUSH_CONSUMER, got %v", protobuf.GetClientType())
	}

	pubsub := protobuf.GetPubSub()
	if pubsub == nil {
		t.Fatal("expected PubSub to be set")
	}

	subscription, ok := pubsub.(*v2.Settings_Subscription)
	if !ok || subscription == nil {
		t.Fatal("expected subscription to be set")
	}

	if subscription.Subscription.GetLiteSubscriptionQuota() != 50 {
		t.Errorf("expected lite subscription quota 50, got %d", subscription.Subscription.GetLiteSubscriptionQuota())
	}

	if subscription.Subscription.GetMaxLiteTopicSize() != 512 {
		t.Errorf("expected max lite topic size 512, got %d", subscription.Subscription.GetMaxLiteTopicSize())
	}

	entry := subscription.Subscription.GetSubscriptions()[0]

	if entry.GetTopic().GetName() != "bind-topic" {
		t.Errorf("expected topic name 'bind-topic', got %s", entry.GetTopic().GetName())
	}

	if entry.GetTopic().GetResourceNamespace() != "test-namespace" {
		t.Errorf("expected namespace 'test-namespace', got %s", entry.GetTopic().GetResourceNamespace())
	}
}
