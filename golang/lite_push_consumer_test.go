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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
)

func TestNewLitePushConsumer(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}

	// 测试成功创建 LitePushConsumer
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}
	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)
	if dlpc.litePushConsumerSettings.bingTopic != "bind-topic" {
		t.Errorf("expected bind topic 'bind-topic', got %s", dlpc.litePushConsumerSettings.bingTopic)
	}

	// 验证 client type 是否正确设置为 LITE_PUSH_CONSUMER
	if int32(dlpc.pcSettings.clientType) != int32(v2.ClientType_LITE_PUSH_CONSUMER) {
		t.Errorf("expected client type LITE_PUSH_CONSUMER, got %v", dlpc.pcSettings.clientType)
	}

	// 验证 isFifo 被强制设置为 true
	if !dlpc.pcSettings.isFifo {
		t.Error("expected isFifo to be true for lite push consumer")
	}
}

func TestNewLitePushConsumer_EmptyBindTopic(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}

	// 测试空 BindTopic 的错误情况
	liteConfig := &LitePushConsumerConfig{BindTopic: ""}
	_, err := NewLitePushConsumer(config, liteConfig)
	if err == nil {
		t.Fatal("expected error for empty bind topic, got nil")
	}

	expectedError := "LitePushConsumerConfig.bindTopic is required"
	if err.Error() != expectedError {
		t.Errorf("expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestLitePushConsumer_SubscribeLite(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)

	// 模拟 consumer 运行状态
	MOCK_DEFAULT_PUSH_CONSUMER.EXPECT().isRunning().Return(true).AnyTimes()

	// Mock SyncLiteSubscription 成功响应
	MOCK_RPC_CLIENT.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
			if req.Action != v2.LiteSubscriptionAction_INCREMENTAL_ADD {
				t.Errorf("expected action INCREMENTAL_ADD, got %v", req.Action)
			}
			if len(req.LiteTopicSet) != 1 || req.LiteTopicSet[0] != "lite-topic-1" {
				t.Errorf("expected lite topic set ['lite-topic-1'], got %v", req.LiteTopicSet)
			}
			return &v2.SyncLiteSubscriptionResponse{
				Status: &v2.Status{Code: v2.Code_OK},
			}, nil
		},
	)

	err = dlpc.SubscribeLite("lite-topic-1")
	if err != nil {
		t.Fatalf("expected no error for SubscribeLite, got %v", err)
	}

	// 验证 lite topic 被添加到 set 中
	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet["lite-topic-1"]; !exists {
		t.Error("expected lite topic to be added to set")
	}
}

func TestLitePushConsumer_SubscribeLite_NotRunning(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)

	// consumer 未运行状态
	MOCK_DEFAULT_PUSH_CONSUMER.EXPECT().isRunning().Return(false).AnyTimes()

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
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)
	MOCK_DEFAULT_PUSH_CONSUMER.EXPECT().isRunning().Return(true).AnyTimes()

	// Mock RPC 错误
	MOCK_RPC_CLIENT.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(
		nil, errors.New("rpc error"),
	)

	err = dlpc.SubscribeLite("lite-topic-1")
	if err == nil {
		t.Fatal("expected rpc error")
	}

	// 验证 lite topic 没有被添加到 set 中
	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet["lite-topic-1"]; exists {
		t.Error("lite topic should not be added when rpc fails")
	}
}

func TestLitePushConsumer_UnSubscribeLite(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)
	MOCK_DEFAULT_PUSH_CONSUMER.EXPECT().isRunning().Return(true).AnyTimes()

	// 预先添加一个 lite topic
	dlpc.litePushConsumerSettings.liteTopicSet["lite-topic-1"] = struct{}{}

	// Mock SyncLiteSubscription 成功响应
	MOCK_RPC_CLIENT.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
			if req.Action != v2.LiteSubscriptionAction_INCREMENTAL_REMOVE {
				t.Errorf("expected action INCREMENTAL_REMOVE, got %v", req.Action)
			}
			if len(req.LiteTopicSet) != 1 || req.LiteTopicSet[0] != "lite-topic-1" {
				t.Errorf("expected lite topic set ['lite-topic-1'], got %v", req.LiteTopicSet)
			}
			return &v2.SyncLiteSubscriptionResponse{
				Status: &v2.Status{Code: v2.Code_OK},
			}, nil
		},
	)

	err = dlpc.UnSubscribeLite("lite-topic-1")
	if err != nil {
		t.Fatalf("expected no error for UnSubscribeLite, got %v", err)
	}

	// 验证 lite topic 被从 set 中删除
	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet["lite-topic-1"]; exists {
		t.Error("expected lite topic to be removed from set")
	}
}

func TestLitePushConsumer_notifyUnsubscribeLite(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)

	// 预先添加一个 lite topic
	dlpc.litePushConsumerSettings.liteTopicSet["lite-topic-notify"] = struct{}{}

	cmd := &v2.NotifyUnsubscribeLiteCommand{
		LiteTopic:  "lite-topic-notify",
		Topic:      "bind-topic",
		Group:      "test-group",
		BrokerName: "test-broker",
	}

	dlpc.notifyUnsubscribeLite(cmd)

	// 验证 lite topic 被从 set 中删除
	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet["lite-topic-notify"]; exists {
		t.Error("expected lite topic to be removed from set after notify")
	}
}

func TestLitePushConsumer_notifyUnsubscribeLite_EmptyLiteTopic(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)

	// 预先添加一个 lite topic
	dlpc.litePushConsumerSettings.liteTopicSet["lite-topic-keep"] = struct{}{}

	cmd := &v2.NotifyUnsubscribeLiteCommand{
		LiteTopic:  "", // 空的 lite topic
		Topic:      "bind-topic",
		Group:      "test-group",
		BrokerName: "test-broker",
	}

	dlpc.notifyUnsubscribeLite(cmd)

	// 验证 lite topic 仍然存在（因为 LiteTopic 为空，函数会提前返回）
	if _, exists := dlpc.litePushConsumerSettings.liteTopicSet["lite-topic-keep"]; !exists {
		t.Error("lite topic should not be removed when command has empty lite topic")
	}
}

func TestLitePushConsumer_syncLiteSubscription_StatusError(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)

	// Mock SyncLiteSubscription 返回错误状态码
	MOCK_RPC_CLIENT.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(
		&v2.SyncLiteSubscriptionResponse{
			Status: &v2.Status{
				Code:    v2.Code_INTERNAL_SERVER_ERROR,
				Message: "internal error",
			},
		}, nil,
	)

	err = dlpc.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_INCREMENTAL_ADD, []string{"test"})
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
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)

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

	if !req.GetAutoRenew() {
		t.Error("expected auto renew to be true for lite push consumer")
	}

	if req.GetAttemptId() == "" {
		t.Error("expected attempt id to be set")
	}

	// 修正 WrapReceiveMessageRequest 的 duration 断言
	if req.GetLongPollingTimeout().GetSeconds() != int64(10) {
		t.Errorf("expected polling timeout 10s, got %v", req.GetLongPollingTimeout().GetSeconds())
	}
}

func TestLitePushConsumer_WrapHeartbeatRequest(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)

	req := dlpc.WrapHeartbeatRequest()

	if req.GetGroup().GetName() != "test-group" {
		t.Errorf("expected group name 'test-group', got %s", req.GetGroup().GetName())
	}

	if req.GetGroup().GetResourceNamespace() != "test-namespace" {
		t.Errorf("expected namespace 'test-namespace', got %s", req.GetGroup().GetResourceNamespace())
	}

	// 修正 WrapHeartbeatRequest 的 clientType 断言
	if int32(req.GetClientType()) != int32(v2.ClientType_LITE_PUSH_CONSUMER) {
		t.Errorf("expected client type LITE_PUSH_CONSUMER, got %v", req.GetClientType())
	}
}

func TestLitePushConsumerSettings_GetMethods(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)
	settings := dlpc.litePushConsumerSettings

	// 测试 GetAccessPoint
	if settings.GetAccessPoint() == nil {
		t.Error("expected access point to be set")
	}

	// 测试 GetClientID
	if settings.GetClientID() == "" {
		t.Error("expected client ID to be set")
	}

	// 测试 GetClientType
	if int32(settings.GetClientType()) != int32(v2.ClientType_LITE_PUSH_CONSUMER) {
		t.Errorf("expected client type LITE_PUSH_CONSUMER, got %v", settings.GetClientType())
	}

	// 测试 GetRequestTimeout
	if settings.GetRequestTimeout() <= 0 {
		t.Error("expected request timeout to be positive")
	}

	// 测试 GetRetryPolicy
	if settings.GetRetryPolicy() == nil {
		t.Error("expected retry policy to be set")
	}
}

func TestLitePushConsumerSettings_applySettingsCommand(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)
	settings := dlpc.litePushConsumerSettings

	// 创建测试 Settings
	liteQuota := int32(100)
	maxSize := int32(1024)

	testSettings := &v2.Settings{
		PubSub: &v2.Settings_Subscription{
			Subscription: &v2.Subscription{
				LiteSubscriptionQuota: &liteQuota,
				MaxLiteTopicSize:      &maxSize,
				Fifo:                  proto.Bool(false), // 这会被强制设置为 true
			},
		},
		BackoffPolicy: &v2.RetryPolicy{
			MaxAttempts: 3,
			ExponentialBackoff: &v2.ExponentialBackoff{
				Initial:    durationpb.New(time.Second),
				Max:        durationpb.New(60 * time.Second),
				Multiplier: 2.0,
			},
		},
	}

	err = settings.applySettingsCommand(testSettings)
	if err != nil {
		t.Fatalf("applySettingsCommand failed: %v", err)
	}

	// 验证 lite subscription quota 被设置
	if settings.liteSubscriptionQuota != liteQuota {
		t.Errorf("expected lite subscription quota %d, got %d", liteQuota, settings.liteSubscriptionQuota)
	}

	// 验证 max lite topic size 被设置
	if settings.maxLiteTopicSize != maxSize {
		t.Errorf("expected max lite topic size %d, got %d", maxSize, settings.maxLiteTopicSize)
	}

	// 验证 isFifo 被强制设置为 true
	if !settings.isFifo {
		t.Error("expected isFifo to be forced to true")
	}
}

func TestLitePushConsumerSettings_toProtobuf(t *testing.T) {
	config := &Config{Endpoint: fakeAddress, NameSpace: "test-namespace", ConsumerGroup: "test-group"}
	liteConfig := &LitePushConsumerConfig{BindTopic: "bind-topic"}

	lpc, err := NewLitePushConsumer(config, liteConfig, WithPushMessageListener(&FuncMessageListener{
		Consume: func(*MessageView) ConsumerResult { return SUCCESS },
	}))
	if err != nil {
		t.Fatalf("failed to create lite push consumer: %v", err)
	}

	dlpc := lpc.(*defaultLitePushConsumer)
	settings := dlpc.litePushConsumerSettings

	// 设置一些测试值
	settings.liteSubscriptionQuota = 50
	settings.maxLiteTopicSize = 512

	protobuf := settings.toProtobuf()

	// 验证 ClientType
	if protobuf.GetClientType() != v2.ClientType_LITE_PUSH_CONSUMER {
		t.Errorf("expected client type LITE_PUSH_CONSUMER, got %v", protobuf.GetClientType())
	}

	// 验证 PubSub 设置
	pubsub := protobuf.GetPubSub()
	if pubsub == nil {
		t.Fatal("expected PubSub to be set")
	}

	subscription := pubsub.(*v2.Settings_Subscription).Subscription
	if subscription == nil {
		t.Fatal("expected subscription to be set")
	}

	// 验证 LiteSubscriptionQuota
	if subscription.GetLiteSubscriptionQuota() != 50 {
		t.Errorf("expected lite subscription quota 50, got %d", subscription.GetLiteSubscriptionQuota())
	}

	// 验证 MaxLiteTopicSize
	if subscription.GetMaxLiteTopicSize() != 512 {
		t.Errorf("expected max lite topic size 512, got %d", subscription.GetMaxLiteTopicSize())
	}

	// 验证订阅信息
	if len(subscription.GetSubscriptions()) == 0 {
		t.Error("expected at least one subscription entry")
	} else {
		entry := subscription.GetSubscriptions()[0]
		// 修正 subscription entry 断言
		if entry.GetTopic().GetName() != "bind-topic" {
			t.Errorf("expected topic name 'bind-topic', got %s", entry.GetTopic().GetName())
		}
		if entry.GetTopic().GetResourceNamespace() != "test-namespace" {
			t.Errorf("expected namespace 'test-namespace', got %s", entry.GetTopic().GetResourceNamespace())
		}
	}
}
