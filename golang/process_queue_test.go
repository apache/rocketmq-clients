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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// noNewMessageStream answers a long polling receive the way an idle queue does:
// a MESSAGE_NOT_FOUND status followed by EOF.
type noNewMessageStream struct {
	grpc.ClientStream
	statusSent bool
}

func (stream *noNewMessageStream) Recv() (*v2.ReceiveMessageResponse, error) {
	if !stream.statusSent {
		stream.statusSent = true
		return &v2.ReceiveMessageResponse{
			Content: &v2.ReceiveMessageResponse_Status{
				Status: &v2.Status{Code: v2.Code_MESSAGE_NOT_FOUND, Message: v2.Code_MESSAGE_NOT_FOUND.String()},
			},
		}, nil
	}
	return nil, io.EOF
}

// An empty long polling result is a completed reception, so it has to close the
// receive hook it opened. Otherwise the inflight count grows for as long as the
// topic stays idle and GracefulStop waits out its whole timeout for nothing.
func TestProcessQueueEmptyLongPollingBalancesInflightReceiveCount(t *testing.T) {
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

	var receptions atomic.Int32
	manager.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
			receptions.Add(1)
			return &noNewMessageStream{}, nil
		}).AnyTimes()

	mq := &v2.MessageQueue{
		Topic:              &v2.Resource{Name: MOCK_TOPIC},
		Id:                 0,
		Broker:             &v2.Broker{Name: "broker", Endpoints: fakeEndpoints()},
		AcceptMessageTypes: []v2.MessageType{v2.MessageType_NORMAL},
	}
	// Registering the queue the way the route scan does keeps the per queue cache
	// threshold above zero, so an idle queue keeps polling instead of backing off
	// as if its cache were full.
	dpq, ok := pc.createProcessQueue(utils.ParseMessageQueue2Str(mq), mq, SUB_ALL).(*defaultProcessQueue)
	if !ok {
		t.Fatal("process queue was not created")
	}
	defer dpq.drop()

	dpq.receiveMessageImmediately()

	const wanted = int32(5)
	deadline := time.Now().Add(10 * time.Second)
	for receptions.Load() < wanted && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := receptions.Load(); got < wanted {
		t.Fatalf("expected at least %d receptions of an idle queue, got %d", wanted, got)
	}

	// Stop the retry loop, then let the receptions in flight report back.
	dpq.drop()
	count := int64(-1)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		count = pc.inflightRequestCountInterceptor.getInflightReceiveRequestCount()
		if count == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count != 0 {
		t.Fatalf("inflight receive count is %d after %d completed receptions of an idle queue; "+
			"an empty long polling result never closed the hook it opened, so GracefulStop would "+
			"wait out requestTimeout + longPollingTimeout before giving up", count, receptions.Load())
	}
}

// newSettlementTestProcessQueue builds a push consumer whose RPCs are served by
// a mock client manager, plus one process queue to drive the settlement chains
// (ack, change invisible duration, forward to DLQ) against it.
func newSettlementTestProcessQueue(t *testing.T) (*defaultPushConsumer, *defaultProcessQueue, *MockClientManager) {
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

func testMessageView() *MessageView {
	return &MessageView{
		messageId:     "message-1",
		topic:         MOCK_TOPIC,
		body:          []byte("body"),
		endpoints:     fakeEndpoints(),
		ReceiptHandle: "receipt-handle-0",
	}
}

// An expired receipt handle can never be renewed, so the chain must abandon at
// once instead of retrying once a second forever.
func TestChangeInvisibleDurationForgivesInvalidReceiptHandle(t *testing.T) {
	_, dpq, manager := newSettlementTestProcessQueue(t)
	defer dpq.drop()

	var calls atomic.Int32
	manager.EXPECT().ChangeInvisibleDuration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ChangeInvisibleDurationRequest, time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
			calls.Add(1)
			return &v2.ChangeInvisibleDurationResponse{
				Status: &v2.Status{Code: v2.Code_INVALID_RECEIPT_HANDLE, Message: v2.Code_INVALID_RECEIPT_HANDLE.String()},
			}, nil
		}).Times(1)

	var got error
	dpq.changeInvisibleDuration(testMessageView(), time.Second, 1, func(err error) { got = err })
	if got == nil {
		t.Fatal("expected the callback to report the abandoned renewal")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected the renewal to be abandoned without a retry, got %d attempts", got)
	}
}

func TestChangeInvisibleDurationRetriesAreCapped(t *testing.T) {
	_, dpq, manager := newSettlementTestProcessQueue(t)
	defer dpq.drop()

	var calls atomic.Int32
	manager.EXPECT().ChangeInvisibleDuration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ChangeInvisibleDurationRequest, time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
			calls.Add(1)
			return nil, errors.New("rpc unavailable")
		}).Times(MAX_CHANGE_INVISIBLE_DURATION_ATTEMPTS)

	done := make(chan error, 1)
	go func() {
		dpq.changeInvisibleDuration(testMessageView(), time.Second, 1, func(err error) { done <- err })
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
	_, dpq, manager := newSettlementTestProcessQueue(t)

	var calls atomic.Int32
	manager.EXPECT().AckMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.AckMessageRequest, time.Duration) (*v2.AckMessageResponse, error) {
			calls.Add(1)
			return nil, errors.New("rpc unavailable")
		}).Times(1)

	done := make(chan error, 1)
	go func() {
		dpq.ackMessage(testMessageView(), func(err error) { done <- err })
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

func TestForwardToDeadLetterQueueForgivesInvalidReceiptHandle(t *testing.T) {
	_, dpq, manager := newSettlementTestProcessQueue(t)
	defer dpq.drop()

	var calls atomic.Int32
	manager.EXPECT().ForwardMessageToDeadLetterQueue(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ForwardMessageToDeadLetterQueueRequest, time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
			calls.Add(1)
			return &v2.ForwardMessageToDeadLetterQueueResponse{
				Status: &v2.Status{Code: v2.Code_INVALID_RECEIPT_HANDLE, Message: v2.Code_INVALID_RECEIPT_HANDLE.String()},
			}, nil
		}).Times(1)

	var got error
	dpq.forwardToDeadLetterQueue(testMessageView(), func(err error) { got = err })
	if got == nil {
		t.Fatal("expected the callback to report the abandoned forward")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected the forward to be abandoned without a retry, got %d attempts", got)
	}
}

func TestForwardToDeadLetterQueueStopsWhenConsumerNotRunning(t *testing.T) {
	pc, dpq, manager := newSettlementTestProcessQueue(t)

	var calls atomic.Int32
	manager.EXPECT().ForwardMessageToDeadLetterQueue(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ForwardMessageToDeadLetterQueueRequest, time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
			calls.Add(1)
			return nil, errors.New("rpc unavailable")
		}).Times(1)

	done := make(chan error, 1)
	go func() {
		dpq.forwardToDeadLetterQueue(testMessageView(), func(err error) { done <- err })
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
