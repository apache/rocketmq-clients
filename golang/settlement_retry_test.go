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
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
)

// newRetryBudgetTestProcessQueue builds a push consumer whose RPCs are served by
// a mock client manager, plus one process queue to drive the settlement chains
// (ack, change invisible duration, forward to DLQ) against it.
func newRetryBudgetTestProcessQueue(t *testing.T) (*defaultPushConsumer, *defaultProcessQueue, *MockClientManager) {
	t.Helper()
	consumer, err := NewPushConsumer(&Config{
		Endpoint:      fakeAddress,
		ConsumerGroup: MOCK_GROUP,
		Credentials:   &credentials.SessionCredentials{},
	},
		WithPushSubscriptionExpressions(map[string]*FilterExpression{MOCK_TOPIC: SUB_ALL}),
		WithPushMessageListener(&FuncMessageListener{
			Consume: func(*MessageView) ConsumerResult { return SUCCESS },
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	pc := consumer.(*defaultPushConsumer)
	manager := NewMockClientManager(gomock.NewController(t))
	pc.cli.clientManager = manager
	pc.cli.log = zap.NewNop().Sugar()
	pc.cli.inited.Store(true)

	mq := &v2.MessageQueue{
		Topic:              &v2.Resource{Name: MOCK_TOPIC},
		Id:                 0,
		Broker:             &v2.Broker{Name: "broker", Endpoints: fakeEndpoints()},
		AcceptMessageTypes: []v2.MessageType{v2.MessageType_NORMAL},
	}
	return pc, newDefaultProcessQueue(pc, utils.ParseMessageQueue2Str(mq), mq, SUB_ALL), manager
}

func retryBudgetTestMessageView() *MessageView {
	return &MessageView{
		messageId:     "message-1",
		topic:         MOCK_TOPIC,
		body:          []byte("body"),
		endpoints:     fakeEndpoints(),
		ReceiptHandle: "receipt-handle-0",
	}
}

func TestChangeInvisibleDurationRetriesAreCapped(t *testing.T) {
	_, dpq, manager := newRetryBudgetTestProcessQueue(t)
	defer dpq.drop()

	var calls atomic.Int32
	manager.EXPECT().ChangeInvisibleDuration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ChangeInvisibleDurationRequest, time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
			calls.Add(1)
			return nil, errors.New("rpc unavailable")
		}).Times(MAX_CHANGE_INVISIBLE_DURATION_ATTEMPTS)

	done := make(chan error, 1)
	go func() {
		dpq.changeInvisibleDuration(retryBudgetTestMessageView(), time.Second, 1, func(err error) { done <- err })
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the callback to report the give-up")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the callback was never invoked after the retry budget was exhausted")
	}
	if got := calls.Load(); got != MAX_CHANGE_INVISIBLE_DURATION_ATTEMPTS {
		t.Fatalf("expected exactly %d attempts, got %d", MAX_CHANGE_INVISIBLE_DURATION_ATTEMPTS, got)
	}
}

// A dropped process queue must not keep issuing settlement RPCs for its cached
// messages; the callback still has to fire so the cache bookkeeping is undone.
func TestAckMessageStopsWhenProcessQueueDropped(t *testing.T) {
	_, dpq, manager := newRetryBudgetTestProcessQueue(t)

	var calls atomic.Int32
	manager.EXPECT().AckMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.AckMessageRequest, time.Duration) (*v2.AckMessageResponse, error) {
			calls.Add(1)
			return nil, errors.New("rpc unavailable")
		}).Times(1)

	done := make(chan error, 1)
	go func() {
		dpq.ackMessage(retryBudgetTestMessageView(), func(err error) { done <- err })
	}()
	time.Sleep(100 * time.Millisecond)
	dpq.drop()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the callback to report the abandoned retry")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the retry guard never released the callback")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected the retry to be abandoned after the first attempt, got %d attempts", got)
	}
}

func TestForwardToDeadLetterQueueStopsWhenConsumerNotRunning(t *testing.T) {
	pc, dpq, manager := newRetryBudgetTestProcessQueue(t)

	var calls atomic.Int32
	manager.EXPECT().ForwardMessageToDeadLetterQueue(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ForwardMessageToDeadLetterQueueRequest, time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
			calls.Add(1)
			return nil, errors.New("rpc unavailable")
		}).Times(1)

	done := make(chan error, 1)
	go func() {
		dpq.forwardToDeadLetterQueue(retryBudgetTestMessageView(), func(err error) { done <- err })
	}()
	time.Sleep(100 * time.Millisecond)
	pc.cli.on.Store(false)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the callback to report the abandoned retry")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the retry guard never released the callback")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected the retry to be abandoned after the first attempt, got %d attempts", got)
	}
}

// A sustained settlement failure must end in the give-up: the ack chain stops
// after MAX_ACK_MESSAGE_ATTEMPTS attempts instead of retrying once a second
// forever.
func TestAckMessageRetriesAreCapped(t *testing.T) {
	_, dpq, manager := newRetryBudgetTestProcessQueue(t)
	defer dpq.drop()

	var calls atomic.Int32
	manager.EXPECT().AckMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.AckMessageRequest, time.Duration) (*v2.AckMessageResponse, error) {
			calls.Add(1)
			return nil, errors.New("rpc unavailable")
		}).Times(MAX_ACK_MESSAGE_ATTEMPTS)

	done := make(chan error, 1)
	go func() {
		dpq.ackMessage(retryBudgetTestMessageView(), func(err error) { done <- err })
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the callback to report the give-up")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the callback was never invoked after the retry budget was exhausted")
	}
	if got := calls.Load(); got != MAX_ACK_MESSAGE_ATTEMPTS {
		t.Fatalf("expected exactly %d attempts, got %d", MAX_ACK_MESSAGE_ATTEMPTS, got)
	}
}
