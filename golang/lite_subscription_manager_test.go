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

func TestLiteSubscriptionManager_SyncAllLiteSubscription(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	m := dlpc.liteSubscriptionManager
	m.liteTopicSet.Store("lite-topic-1", struct{}{})
	m.liteTopicSet.Store("lite-topic-2", struct{}{})

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
		if req.GetAction() != v2.LiteSubscriptionAction_COMPLETE_ADD {
			t.Errorf("expected action COMPLETE_ADD, got %v", req.GetAction())
		}
		topics := req.GetLiteTopicSet()
		if len(topics) != 2 {
			t.Errorf("expected 2 lite topics, got %v", topics)
		}
		got := map[string]bool{}
		for _, topic := range topics {
			got[topic] = true
		}
		if !got["lite-topic-1"] || !got["lite-topic-2"] {
			t.Errorf("expected full lite topic set, got %v", topics)
		}
		return setupSuccessResponse(), nil
	}).Times(1)

	m.syncAllLiteSubscription()
}

func TestLiteSubscriptionManager_SyncAllLiteSubscription_QuotaExceeded(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	m := dlpc.liteSubscriptionManager
	m.liteSubscriptionQuota = 1
	m.liteTopicSet.Store("lite-topic-1", struct{}{})
	m.liteTopicSet.Store("lite-topic-2", struct{}{})

	// no EXPECT registered: any SyncLiteSubscription call would fail the test
	m.syncAllLiteSubscription()
}

func TestLiteSubscriptionManager_StartUp(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
		if req.GetAction() != v2.LiteSubscriptionAction_COMPLETE_ADD {
			t.Errorf("expected action COMPLETE_ADD on startUp, got %v", req.GetAction())
		}
		if len(req.GetLiteTopicSet()) != 0 {
			t.Errorf("expected empty lite topic set on startUp, got %v", req.GetLiteTopicSet())
		}
		return setupSuccessResponse(), nil
	}).Times(1)

	dlpc.liteSubscriptionManager.startUp()
	// stop the 30s ticker goroutine; no further sync should have happened
	close(dlpc.cli.done)
}

func TestLiteSubscriptionManager_SyncLiteSubscription_MultiEndpointFirstErr(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	queues := []*v2.MessageQueue{
		{Broker: &v2.Broker{Endpoints: &v2.Endpoints{
			Scheme:    v2.AddressScheme_IPv4,
			Addresses: []*v2.Address{{Host: "127.0.0.1", Port: 80}},
		}}},
		{Broker: &v2.Broker{Endpoints: &v2.Endpoints{
			Scheme:    v2.AddressScheme_IPv4,
			Addresses: []*v2.Address{{Host: "127.0.0.2", Port: 81}},
		}}},
	}
	dlpc.cli.router.Store("bind-topic", queues)

	firstErr := errors.New("endpoint-1 down")
	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(nil, firstErr).Times(1)
	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(setupSuccessResponse(), nil).Times(1)

	err = dlpc.liteSubscriptionManager.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_PARTIAL_ADD, []string{"lite-topic-1"}, nil)
	if err == nil {
		t.Fatal("expected first endpoint error to be returned")
	}
	if err.Error() != firstErr.Error() {
		t.Errorf("expected first error '%v', got '%v'", firstErr, err)
	}
}

func TestLiteSubscriptionManager_GetRouteEndpoints(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}
	m := dlpc.liteSubscriptionManager

	t.Run("no route data", func(t *testing.T) {
		if endpoints := m.getRouteEndpoints(); endpoints != nil {
			t.Errorf("expected nil endpoints without route data, got %v", endpoints)
		}
	})

	t.Run("unexpected route type", func(t *testing.T) {
		dlpc.cli.router.Store("bind-topic", "not-message-queues")
		if endpoints := m.getRouteEndpoints(); endpoints != nil {
			t.Errorf("expected nil endpoints for unexpected route type, got %v", endpoints)
		}
	})

	t.Run("deduplicate endpoints", func(t *testing.T) {
		sharedEndpoints := &v2.Endpoints{
			Scheme:    v2.AddressScheme_IPv4,
			Addresses: []*v2.Address{{Host: "127.0.0.1", Port: 80}},
		}
		otherEndpoints := &v2.Endpoints{
			Scheme:    v2.AddressScheme_IPv4,
			Addresses: []*v2.Address{{Host: "127.0.0.2", Port: 81}},
		}
		dlpc.cli.router.Store("bind-topic", []*v2.MessageQueue{
			{Broker: &v2.Broker{Endpoints: sharedEndpoints}},
			{Broker: &v2.Broker{Endpoints: sharedEndpoints}},
			{Broker: &v2.Broker{Endpoints: otherEndpoints}},
		})
		endpoints := m.getRouteEndpoints()
		if len(endpoints) != 2 {
			t.Errorf("expected 2 unique endpoints after dedup, got %d", len(endpoints))
		}
	})
}

func TestLiteSubscriptionManager_ValidateLiteTopic(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}
	m := dlpc.liteSubscriptionManager

	if err := m.validateLiteTopic("   "); err == nil || err.Error() != "liteTopic is blank" {
		t.Errorf("expected 'liteTopic is blank' error, got %v", err)
	}

	m.maxLiteTopicSize = 4
	if err := m.validateLiteTopic("abcde"); err == nil || !strings.Contains(err.Error(), "exceeded max length 4") {
		t.Errorf("expected max length exceeded error, got %v", err)
	}
	if err := m.validateLiteTopic("abcd"); err != nil {
		t.Errorf("expected no error for topic within max length, got %v", err)
	}
}

func TestLiteSubscriptionManager_SubscribeLite_QuotaExceeded(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	m := dlpc.liteSubscriptionManager
	m.liteSubscriptionQuota = 1
	m.liteTopicSet.Store("lite-topic-1", struct{}{})

	// no EXPECT registered: quota must be rejected before any RPC
	err = dlpc.SubscribeLite("lite-topic-2")
	if err == nil {
		t.Fatal("expected quota exceeded error")
	}
	rpcErr, ok := err.(*ErrRpcStatus)
	if !ok {
		t.Fatalf("expected ErrRpcStatus, got %T", err)
	}
	if rpcErr.Code != int32(v2.Code_LITE_SUBSCRIPTION_QUOTA_EXCEEDED) {
		t.Errorf("expected code LITE_SUBSCRIPTION_QUOTA_EXCEEDED, got %d", rpcErr.Code)
	}
}

func TestLiteSubscriptionManager_SubscribeLite_MultipleOffsetOptions(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	err = dlpc.SubscribeLite("lite-topic-1", LastOffset, MinOffset)
	if err == nil || err.Error() != "only one offset option is supported" {
		t.Errorf("expected 'only one offset option is supported' error, got %v", err)
	}
}

func TestLiteSubscriptionManager_SubscribeLite_Duplicate(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	dlpc.liteSubscriptionManager.liteTopicSet.Store("lite-topic-1", struct{}{})

	// no EXPECT registered: duplicate subscribe must be a no-op without RPC
	if err = dlpc.SubscribeLite("lite-topic-1"); err != nil {
		t.Errorf("expected no error for duplicate subscribe, got %v", err)
	}
}

func TestLiteSubscriptionManager_SubscribeLite_BlankTopic(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	err = dlpc.SubscribeLite("  ")
	if err == nil || err.Error() != "liteTopic is blank" {
		t.Errorf("expected 'liteTopic is blank' error, got %v", err)
	}
}

func TestLiteSubscriptionManager_UnsubscribeLite_NotSubscribed(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	// no EXPECT registered: unsubscribing an unknown topic must be a no-op without RPC
	if err = dlpc.UnSubscribeLite("unknown-topic"); err != nil {
		t.Errorf("expected no error for unsubscribing unknown topic, got %v", err)
	}
}

func TestLiteSubscriptionManager_UnsubscribeLite_NotRunning(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	dlpc.cli.on.Store(false)

	err = dlpc.UnSubscribeLite("lite-topic-1")
	if err == nil || !strings.HasPrefix(err.Error(), "client not running, clientId=") {
		t.Errorf("expected client not running error, got %v", err)
	}
}

func TestLiteSubscriptionManager_UnsubscribeLite_RpcError(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	dlpc.liteSubscriptionManager.liteTopicSet.Store("lite-topic-1", struct{}{})

	mockRpcClient.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any()).Return(nil, errors.New("rpc error"))

	err = dlpc.UnSubscribeLite("lite-topic-1")
	if err == nil {
		t.Fatal("expected rpc error")
	}
	if _, exists := dlpc.liteSubscriptionManager.liteTopicSet.Load("lite-topic-1"); !exists {
		t.Error("lite topic should be kept when rpc fails")
	}
}

func TestLiteSubscriptionManager_Sync(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}
	m := dlpc.liteSubscriptionManager

	t.Run("nil subscription keeps defaults", func(t *testing.T) {
		m.sync(nil)
		if m.liteSubscriptionQuota != 2000 || m.maxLiteTopicSize != 64 {
			t.Errorf("expected defaults kept, got quota=%d maxSize=%d", m.liteSubscriptionQuota, m.maxLiteTopicSize)
		}
	})

	t.Run("nil fields keep previous values", func(t *testing.T) {
		m.sync(&v2.Subscription{})
		if m.liteSubscriptionQuota != 2000 || m.maxLiteTopicSize != 64 {
			t.Errorf("expected previous values kept, got quota=%d maxSize=%d", m.liteSubscriptionQuota, m.maxLiteTopicSize)
		}
	})

	t.Run("server pushed values applied", func(t *testing.T) {
		quota := int32(100)
		maxSize := int32(128)
		m.sync(&v2.Subscription{
			LiteSubscriptionQuota: &quota,
			MaxLiteTopicSize:      &maxSize,
		})
		if m.liteSubscriptionQuota != quota || m.maxLiteTopicSize != maxSize {
			t.Errorf("expected quota=%d maxSize=%d, got quota=%d maxSize=%d", quota, maxSize, m.liteSubscriptionQuota, m.maxLiteTopicSize)
		}
	})
}

func TestLitePushConsumer_GetLiteTopicSet(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	dlpc.liteSubscriptionManager.liteTopicSet.Store("lite-topic-1", struct{}{})
	// non-string keys must be skipped defensively
	dlpc.liteSubscriptionManager.liteTopicSet.Store(42, struct{}{})

	topics := dlpc.GetLiteTopicSet()
	if len(topics) != 1 || topics[0] != "lite-topic-1" {
		t.Errorf("expected ['lite-topic-1'], got %v", topics)
	}
}

func TestNewLitePushConsumerConfig(t *testing.T) {
	liteConfig := NewLitePushConsumerConfig("bind-topic", 30*time.Second)
	if liteConfig.bindTopic != "bind-topic" {
		t.Errorf("expected bind topic 'bind-topic', got %s", liteConfig.bindTopic)
	}
	if liteConfig.invisibleDuration != 30*time.Second {
		t.Errorf("expected invisible duration 30s, got %v", liteConfig.invisibleDuration)
	}
}

func TestLitePushConsumerSettings_applySettingsCommand_NilPubSub(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	err = dlpc.litePushConsumerSettings.applySettingsCommand(&v2.Settings{})
	if err == nil {
		t.Fatal("expected error for settings without pubsub")
	}
}

func TestPushConsumer_IsRunning_Stopping(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	dlpc, err := createTestLitePushConsumer(t)
	if err != nil {
		t.Fatalf("failed to create test lite push consumer: %v", err)
	}

	if !dlpc.isRunning() {
		t.Error("expected isRunning true when client is on")
	}
	dlpc.stopping.Store(true)
	if dlpc.isRunning() {
		t.Error("expected isRunning false when stopping is in progress")
	}
}

// clientCore contract tests: isRunning/getClient/getRequestTimeout moved down
// from consumer implementations must stay consistent across client types.
func TestClientCore_ProducerAccessors(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	p, err := NewProducer(&Config{Endpoint: fakeAddress})
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	dp := p.(*defaultProducer)

	if dp.getClient() != dp.cli {
		t.Error("expected getClient to return the underlying client")
	}
	if dp.getRequestTimeout() != dp.pSetting.requestTimeout {
		t.Error("expected getRequestTimeout to return producer settings request timeout")
	}
	// on is initialized to true at construction and turned off at shutdown
	if !dp.isRunning() {
		t.Error("expected isRunning true when client is on")
	}
	dp.cli.on.Store(false)
	if dp.isRunning() {
		t.Error("expected isRunning false when client is off")
	}
}

func TestClientCore_SimpleConsumerAccessors(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	sc, err := NewSimpleConsumer(&Config{Endpoint: fakeAddress, ConsumerGroup: "test-group"})
	if err != nil {
		t.Fatalf("failed to create simple consumer: %v", err)
	}
	dsc := sc.(*defaultSimpleConsumer)

	if dsc.getClient() != dsc.cli {
		t.Error("expected getClient to return the underlying client")
	}
	if dsc.getRequestTimeout() != dsc.scSettings.requestTimeout {
		t.Error("expected getRequestTimeout to return simple consumer settings request timeout")
	}
	// on is initialized to true at construction and turned off at shutdown
	if !dsc.isRunning() {
		t.Error("expected isRunning true when client is on")
	}
	dsc.cli.on.Store(false)
	if dsc.isRunning() {
		t.Error("expected isRunning false when client is off")
	}
}
