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
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
)

// fifoRecorder keeps the order in which messages are consumed and settled.
type fifoRecorder struct {
	mu       sync.Mutex
	consumed []string
	acked    []string
	suspends []string
	dlq      []string
}

func (r *fifoRecorder) add(list *[]string, v string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*list = append(*list, v)
}

func (r *fifoRecorder) snapshot() (consumed, acked, suspends []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.consumed...), append([]string(nil), r.acked...), append([]string(nil), r.suspends...)
}

func (r *fifoRecorder) settled() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(append(append([]string(nil), r.acked...), r.suspends...), r.dlq...)
}

func (r *fifoRecorder) waitSettled(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(r.settled()) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	consumed, _, _ := r.snapshot()
	t.Fatalf("expected %d settled messages, consumed=%v settled=%v", n, consumed, r.settled())
}

// newFifoTestConsumer builds a push consumer whose FIFO consume service and
// process queue run for real; only the RPCs to the server are mocked.
func newFifoTestConsumer(t *testing.T, clientType v2.ClientType, consume func(*MessageView) ConsumerResult,
	newService func(*defaultPushConsumer, *simpleThreadPool) ConsumeService) (*defaultPushConsumer, ProcessQueue, *fifoRecorder) {
	t.Helper()
	recorder := &fifoRecorder{}
	consumer, err := NewPushConsumer(&Config{
		Endpoint:      fakeAddress,
		ConsumerGroup: MOCK_GROUP,
		Credentials:   &credentials.SessionCredentials{},
	},
		WithPushSubscriptionExpressions(map[string]*FilterExpression{MOCK_TOPIC: SUB_ALL}),
		WithPushMessageListener(&FuncMessageListener{Consume: func(mv *MessageView) ConsumerResult {
			recorder.add(&recorder.consumed, fmt.Sprintf("%s@%d", mv.GetMessageId(), mv.GetDeliveryAttempt()))
			return consume(mv)
		}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	pc := consumer.(*defaultPushConsumer)
	manager := NewMockClientManager(gomock.NewController(t))
	pc.cli.clientManager = manager
	pc.cli.log = zap.NewNop().Sugar()
	pc.cli.on.Store(true)
	pc.pcSettings.clientType = clientType
	pc.pcSettings.isFifo = true
	pc.pcSettings.retryPolicy = &v2.RetryPolicy{
		MaxAttempts: 3,
		Strategy: &v2.RetryPolicy_CustomizedBackoff{CustomizedBackoff: &v2.CustomizedBackoff{
			Next: []*durationpb.Duration{durationpb.New(20 * time.Millisecond)},
		}},
	}

	manager.EXPECT().AckMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *v2.Endpoints, req *v2.AckMessageRequest, _ time.Duration) (*v2.AckMessageResponse, error) {
			recorder.add(&recorder.acked, req.GetEntries()[0].GetMessageId())
			return &v2.AckMessageResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
		}).AnyTimes()
	manager.EXPECT().ChangeInvisibleDuration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *v2.Endpoints, req *v2.ChangeInvisibleDurationRequest, _ time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
			recorder.add(&recorder.suspends, req.GetMessageId())
			return &v2.ChangeInvisibleDurationResponse{Status: &v2.Status{Code: v2.Code_OK}, ReceiptHandle: "new-handle"}, nil
		}).AnyTimes()
	manager.EXPECT().ForwardMessageToDeadLetterQueue(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *v2.Endpoints, req *v2.ForwardMessageToDeadLetterQueueRequest, _ time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
			recorder.add(&recorder.dlq, req.GetMessageId())
			return &v2.ForwardMessageToDeadLetterQueueResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
		}).AnyTimes()

	threadPool := NewSimpleThreadPool("MessageConsumption", 64, 4)
	t.Cleanup(threadPool.Shutdown)
	pc.consumerService = newService(pc, threadPool)
	mq := fifoTestMessageQueue()
	pq := pc.createProcessQueue(utils.ParseMessageQueue2Str(mq), mq, SUB_ALL)
	if pq == nil {
		t.Fatal("process queue was not created")
	}
	t.Cleanup(pq.drop)
	return pc, pq, recorder
}

func fifoTestMessageQueue() *v2.MessageQueue {
	return &v2.MessageQueue{
		Topic:              &v2.Resource{Name: MOCK_TOPIC},
		Broker:             &v2.Broker{Name: "broker", Endpoints: fakeEndpoints()},
		AcceptMessageTypes: []v2.MessageType{v2.MessageType_FIFO, v2.MessageType_LITE},
	}
}

func fifoTestMessage(id, liteTopic string) *MessageView {
	return &MessageView{
		messageId:       id,
		topic:           MOCK_TOPIC,
		liteTopic:       liteTopic,
		messageGroup:    &liteTopic,
		ReceiptHandle:   "handle-" + id,
		deliveryAttempt: 1,
		endpoints:       fakeEndpoints(),
		messageQueue:    fifoTestMessageQueue(),
	}
}

// filterByPrefix keeps the records of one lite topic or message group, whose
// message ids all start with the same letter.
func filterByPrefix(records []string, prefix byte) []string {
	var out []string
	for _, r := range records {
		if r[0] == prefix {
			out = append(out, r)
		}
	}
	return out
}

// A failed FIFO message is redelivered locally. The messages behind it in the
// same group must wait until it is settled, otherwise FIFO order is broken.
func TestFifoConsumeServiceRetriesFailedMessageBeforeNextMessage(t *testing.T) {
	services := []struct {
		name        string
		clientType  v2.ClientType
		accelerator bool
		newService  func(*defaultPushConsumer, *simpleThreadPool, bool) ConsumeService
	}{
		{"fifo", v2.ClientType_PUSH_CONSUMER, false, func(pc *defaultPushConsumer, tp *simpleThreadPool, acc bool) ConsumeService {
			return NewFiFoConsumeService(pc.cli.clientID, pc.pcOpts.messageListener, tp, pc.cli, acc)
		}},
		{"fifo with accelerator", v2.ClientType_PUSH_CONSUMER, true, func(pc *defaultPushConsumer, tp *simpleThreadPool, acc bool) ConsumeService {
			return NewFiFoConsumeService(pc.cli.clientID, pc.pcOpts.messageListener, tp, pc.cli, acc)
		}},
		{"lite fifo", v2.ClientType_LITE_PUSH_CONSUMER, false, func(pc *defaultPushConsumer, tp *simpleThreadPool, acc bool) ConsumeService {
			return NewLiteFifoConsumeService(pc.cli.clientID, pc.pcOpts.messageListener, tp, pc.cli, acc)
		}},
		{"lite fifo with accelerator", v2.ClientType_LITE_PUSH_CONSUMER, true, func(pc *defaultPushConsumer, tp *simpleThreadPool, acc bool) ConsumeService {
			return NewLiteFifoConsumeService(pc.cli.clientID, pc.pcOpts.messageListener, tp, pc.cli, acc)
		}},
	}
	for _, svc := range services {
		t.Run(svc.name, func(t *testing.T) {
			var mu sync.Mutex
			failures := map[string]int{"a0": 2}
			_, pq, recorder := newFifoTestConsumer(t, svc.clientType, func(mv *MessageView) ConsumerResult {
				mu.Lock()
				defer mu.Unlock()
				if failures[mv.GetMessageId()] > 0 {
					failures[mv.GetMessageId()]--
					return FAILURE
				}
				return SUCCESS
			}, func(pc *defaultPushConsumer, tp *simpleThreadPool) ConsumeService {
				return svc.newService(pc, tp, svc.accelerator)
			})

			// Group a fails twice on its first message; group b is interleaved and
			// must not be affected.
			batch := []*MessageView{
				fifoTestMessage("a0", "a"), fifoTestMessage("b0", "b"), fifoTestMessage("a1", "a"),
				fifoTestMessage("b1", "b"), fifoTestMessage("a2", "a"),
			}
			pq.(*defaultProcessQueue).consumer.consumerService.consume(pq, batch)
			recorder.waitSettled(t, len(batch))

			consumed, acked, _ := recorder.snapshot()
			if want := []string{"a0@1", "a0@2", "a0@3", "a1@1", "a2@1"}; !reflect.DeepEqual(filterByPrefix(consumed, 'a'), want) {
				t.Fatalf("group a consumed out of order: got %v, want %v (all: %v)", filterByPrefix(consumed, 'a'), want, consumed)
			}
			if want := []string{"a0", "a1", "a2"}; !reflect.DeepEqual(filterByPrefix(acked, 'a'), want) {
				t.Fatalf("group a acked out of order: got %v, want %v", filterByPrefix(acked, 'a'), want)
			}
			if want := []string{"b0", "b1"}; !reflect.DeepEqual(filterByPrefix(acked, 'b'), want) {
				t.Fatalf("group b acked out of order: got %v, want %v", filterByPrefix(acked, 'b'), want)
			}
		})
	}
}

// A FIFO message that runs out of attempts goes to the dead letter queue before
// the next message of the group is consumed.
func TestFifoConsumeServiceForwardsToDeadLetterQueueBeforeNextMessage(t *testing.T) {
	_, pq, recorder := newFifoTestConsumer(t, v2.ClientType_PUSH_CONSUMER, func(mv *MessageView) ConsumerResult {
		if mv.GetMessageId() == "a0" {
			return FAILURE
		}
		return SUCCESS
	}, func(pc *defaultPushConsumer, tp *simpleThreadPool) ConsumeService {
		return NewFiFoConsumeService(pc.cli.clientID, pc.pcOpts.messageListener, tp, pc.cli, false)
	})

	batch := []*MessageView{fifoTestMessage("a0", "a"), fifoTestMessage("a1", "a")}
	pq.(*defaultProcessQueue).consumer.consumerService.consume(pq, batch)
	recorder.waitSettled(t, len(batch))

	consumed, acked, _ := recorder.snapshot()
	if want := []string{"a0@1", "a0@2", "a0@3", "a1@1"}; !reflect.DeepEqual(consumed, want) {
		t.Fatalf("consumed: got %v, want %v", consumed, want)
	}
	if want := []string{"a1"}; !reflect.DeepEqual(acked, want) {
		t.Fatalf("acked: got %v, want %v", acked, want)
	}
	if want := []string{"a1", "a0"}; !reflect.DeepEqual(recorder.settled(), want) {
		t.Fatalf("acked + suspended + dead lettered: got %v, want %v", recorder.settled(), want)
	}
}

// A suspended lite message suspends the rest of its lite topic in the batch and
// the consumption goes on with the other lite topics.
func TestLiteFifoConsumeServiceSuspendSkipsSameLiteTopic(t *testing.T) {
	_, pq, recorder := newFifoTestConsumer(t, v2.ClientType_LITE_PUSH_CONSUMER, func(mv *MessageView) ConsumerResult {
		if mv.GetMessageId() == "a0" {
			return NewConsumerResultSuspend(time.Second)
		}
		return SUCCESS
	}, func(pc *defaultPushConsumer, tp *simpleThreadPool) ConsumeService {
		return NewLiteFifoConsumeService(pc.cli.clientID, pc.pcOpts.messageListener, tp, pc.cli, false)
	})

	batch := []*MessageView{
		fifoTestMessage("a0", "a"), fifoTestMessage("b0", "b"), fifoTestMessage("a1", "a"), fifoTestMessage("b1", "b"),
	}
	pq.(*defaultProcessQueue).consumer.consumerService.consume(pq, batch)
	recorder.waitSettled(t, len(batch))

	consumed, acked, suspends := recorder.snapshot()
	if want := []string{"a0@1", "b0@1", "b1@1"}; !reflect.DeepEqual(consumed, want) {
		t.Fatalf("consumed: got %v, want %v", consumed, want)
	}
	if want := []string{"b0", "b1"}; !reflect.DeepEqual(acked, want) {
		t.Fatalf("acked: got %v, want %v", acked, want)
	}
	if want := []string{"a0", "a1"}; !reflect.DeepEqual(suspends, want) {
		t.Fatalf("suspended: got %v, want %v", suspends, want)
	}
}
