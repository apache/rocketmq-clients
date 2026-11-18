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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var assignmentConsumerModes = []struct {
	name        string
	fifo        bool
	lite        bool
	accelerator bool
}{
	{name: "standard"},
	{name: "fifo", fifo: true},
	{name: "fifo_accelerated", fifo: true, accelerator: true},
	{name: "lite", fifo: true, lite: true},
	{name: "lite_accelerated", fifo: true, lite: true, accelerator: true},
}

func newAssignmentTestConsumer(t *testing.T, mode int) (*defaultPushConsumer, *MockClientManager, *v2.MessageQueue) {
	t.Helper()
	config := &Config{Endpoint: fakeAddress, ConsumerGroup: MOCK_GROUP}
	options := []PushConsumerOption{
		WithPushSubscriptionExpressions(map[string]*FilterExpression{MOCK_TOPIC: SUB_ALL}),
		WithPushMessageListener(&FuncMessageListener{Consume: func(*MessageView) ConsumerResult { return SUCCESS }}),
		WithPushEnableFifoConsumeAccelerator(assignmentConsumerModes[mode].accelerator),
	}
	var pc *defaultPushConsumer
	if assignmentConsumerModes[mode].lite {
		consumer, err := NewLitePushConsumer(config, NewLitePushConsumerConfig(MOCK_TOPIC, 30*time.Second), options...)
		if err != nil {
			t.Fatal(err)
		}
		pc = consumer.(*defaultLitePushConsumer).defaultPushConsumer
	} else {
		consumer, err := newPushConsumer(config, options...)
		if err != nil {
			t.Fatal(err)
		}
		pc = consumer
		pc.pcSettings.isFifo = assignmentConsumerModes[mode].fifo
	}
	manager := NewMockClientManager(gomock.NewController(t))
	pc.cli.clientManager = manager
	pc.cli.log = zap.NewNop().Sugar()
	pc.cli.inited.Store(true)
	t.Cleanup(func() {
		pc.stopAssignmentScan()
		pc.cli.cancel()
	})
	mq := &v2.MessageQueue{
		Topic:              &v2.Resource{Name: MOCK_TOPIC},
		Id:                 0,
		Broker:             &v2.Broker{Name: "broker", Endpoints: fakeEndpoints()},
		AcceptMessageTypes: []v2.MessageType{v2.MessageType_NORMAL},
	}
	return pc, manager, mq
}

func TestPushAssignmentQueryFailurePreservesQueues(t *testing.T) {
	cases := []struct {
		name     string
		response *v2.QueryAssignmentResponse
		err      error
	}{
		{name: "transport", err: errors.New("transport unavailable")},
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "cancellation", err: context.Canceled},
		{name: "server_rejection", response: &v2.QueryAssignmentResponse{Status: &v2.Status{Code: v2.Code_BAD_REQUEST}}},
	}
	for mode, consumerMode := range assignmentConsumerModes {
		t.Run(consumerMode.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					pc, manager, mq := newAssignmentTestConsumer(t, mode)
					previous := []*v2.Assignment{{MessageQueue: mq}}
					pc.cacheAssignments.Store(MOCK_TOPIC, &previous)
					pq := pc.createProcessQueue(utils.ParseMessageQueue2Str(mq), mq, SUB_ALL).(*defaultProcessQueue)
					manager.EXPECT().QueryAssignments(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tc.response, tc.err)
					pc.scanAssignments()
					if cached, _ := pc.cacheAssignments.Load(MOCK_TOPIC); cached != &previous {
						t.Fatal("query failure replaced the previous assignment")
					}
					if _, exists := pc.processQueueTable.Load(pq.mqstr); !exists || pq.dropped.Load() {
						t.Fatal("query failure dropped the existing process queue")
					}
					// A subsequent successful empty response is authoritative.
					manager.EXPECT().QueryAssignments(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(
						&v2.QueryAssignmentResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil)
					pc.scanAssignments()
					if _, exists := pc.processQueueTable.Load(pq.mqstr); exists || !pq.dropped.Load() {
						t.Fatal("successful empty assignment did not remove the old queue")
					}
					if cached, _ := pc.cacheAssignments.Load(MOCK_TOPIC); !utils.IsAssignmentsEmpty(cached.(*[]*v2.Assignment)) {
						t.Fatal("successful empty assignment did not replace the cached assignment")
					}
				})
			}
		})
	}
}

type assignmentShutdownObserver struct {
	ConsumeService
	shutdown func() error
}

func (observer *assignmentShutdownObserver) Shutdown() error {
	return observer.shutdown()
}

func waitAssignmentSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for assignment scan")
	}
}

func TestPushAssignmentStopCancelsAndJoinsLateResponse(t *testing.T) {
	for mode, consumerMode := range assignmentConsumerModes {
		t.Run(consumerMode.name, func(t *testing.T) {
			pc, manager, mq := newAssignmentTestConsumer(t, mode)
			pc.subscriptionExpressions.Store("other-topic", SUB_ALL)
			previous := []*v2.Assignment{{MessageQueue: mq}}
			pc.cacheAssignments.Store(MOCK_TOPIC, &previous)
			pq := pc.createProcessQueue(utils.ParseMessageQueue2Str(mq), mq, SUB_ALL).(*defaultProcessQueue)
			replacement := proto.Clone(mq).(*v2.MessageQueue)
			replacement.Id++
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			manager.EXPECT().QueryAssignments(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ *v2.Endpoints, _ *v2.QueryAssignmentRequest, _ time.Duration) (*v2.QueryAssignmentResponse, error) {
					close(entered)
					<-ctx.Done()
					close(canceled)
					// Model a completed response that arrives after cancellation.
					<-release
					return &v2.QueryAssignmentResponse{
						Status: &v2.Status{Code: v2.Code_OK}, Assignments: []*v2.Assignment{{MessageQueue: replacement}},
					}, nil
				})
			executorStopped := make(chan struct{})
			pc.consumerService = &assignmentShutdownObserver{shutdown: func() error {
				select {
				case <-pc.assignmentScanDone:
				default:
					t.Error("consumption executor shut down before the assignment scan finished")
				}
				if pc.cli.ctx.Err() != nil {
					t.Error("client canceled before consumption/ACK cleanup")
				}
				close(executorStopped)
				return nil
			}}
			manager.EXPECT().shutdown()
			pc.startAssignmentScan(0, time.Hour)
			// Duplicate registration must not create another scanner.
			pc.startAssignmentScan(0, time.Hour)
			waitAssignmentSignal(t, entered)
			stopDone := make(chan struct{})
			stopResult := make(chan error, 1)
			go func() {
				defer close(stopDone)
				stopResult <- pc.GracefulStop()
			}()
			t.Cleanup(func() { unblock(); waitAssignmentSignal(t, stopDone) })
			waitAssignmentSignal(t, canceled)
			select {
			case <-executorStopped:
				t.Fatal("shutdown did not wait for the in-flight assignment scan")
			case <-time.After(50 * time.Millisecond):
			}
			unblock()
			waitAssignmentSignal(t, stopDone)
			if err := <-stopResult; err != nil {
				t.Fatal(err)
			}
			if cached, _ := pc.cacheAssignments.Load(MOCK_TOPIC); cached != &previous {
				t.Fatal("late response changed cached assignments after stopping")
			}
			if _, exists := pc.processQueueTable.Load(pq.mqstr); !exists || pq.dropped.Load() || pc.getQueueSize() != 1 {
				t.Fatal("late response replaced the process queue after stopping")
			}
			if pc.cli.ctx.Err() == nil {
				t.Fatal("client was not canceled after graceful shutdown")
			}
		})
	}
}

func TestPushAssignmentStopDuringDelayAndRegistration(t *testing.T) {
	pc, _, _ := newAssignmentTestConsumer(t, 0)
	pc.startAssignmentScan(time.Hour, time.Hour)
	pc.stopAssignmentScan()
	waitAssignmentSignal(t, pc.assignmentScanDone)

	for i := 0; i < 50; i++ {
		pc, _, _ := newAssignmentTestConsumer(t, 0)
		var workers sync.WaitGroup
		workers.Add(2)
		go func() { defer workers.Done(); pc.startAssignmentScan(time.Hour, time.Hour) }()
		go func() { defer workers.Done(); pc.stopAssignmentScan() }()
		joined := make(chan struct{})
		go func() { workers.Wait(); close(joined) }()
		waitAssignmentSignal(t, joined)
		// Stop also prevents registration when it won before startup.
		done := pc.assignmentScanDone
		pc.startAssignmentScan(0, time.Hour)
		if pc.assignmentScanDone != done {
			t.Fatal("scanner registered after stopping")
		}
		if done != nil {
			select {
			case <-done:
			default:
				t.Fatal("stop returned with a scanner still waiting in its initial delay")
			}
		}
	}
}

func TestPushAssignmentRetryIsSerial(t *testing.T) {
	pc, manager, mq := newAssignmentTestConsumer(t, 0)
	previous := []*v2.Assignment{{MessageQueue: mq}}
	pc.cacheAssignments.Store(MOCK_TOPIC, &previous)
	pq := pc.createProcessQueue(utils.ParseMessageQueue2Str(mq), mq, SUB_ALL).(*defaultProcessQueue)
	entered, release, retried := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, retryOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls, active atomic.Int32
	manager.EXPECT().QueryAssignments(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *v2.Endpoints, *v2.QueryAssignmentRequest, time.Duration) (*v2.QueryAssignmentResponse, error) {
			if active.Add(1) != 1 {
				t.Error("assignment queries overlapped")
			}
			defer active.Add(-1)
			attempt := calls.Add(1)
			if attempt == 1 {
				return nil, errors.New("temporary assignment query failure")
			}
			if attempt == 2 {
				close(entered)
				<-release
			}
			retryOnce.Do(func() { close(retried) })
			return &v2.QueryAssignmentResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
		}).AnyTimes()
	pc.startAssignmentScan(0, time.Millisecond)
	waitAssignmentSignal(t, entered)
	if pq.dropped.Load() {
		t.Fatal("queue dropped while assignment query was in flight")
	}
	// Keep a periodic query blocked across several ticks.
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 2 {
		t.Fatal("another periodic query started while the previous one was blocked")
	}
	unblock()
	waitAssignmentSignal(t, retried)
	waitForCondition(t, 5*time.Second, pq.dropped.Load, "the successful empty retry to remove the old queue")
	pc.stopAssignmentScan()
	if calls.Load() < 2 || !pq.dropped.Load() {
		t.Fatal("periodic scan did not apply the successful empty retry")
	}
}

func TestPushAssignmentClientCancellationStopsScan(t *testing.T) {
	pc, _, _ := newAssignmentTestConsumer(t, 0)
	pc.startAssignmentScan(time.Hour, time.Hour)
	pc.cli.cancel()
	waitAssignmentSignal(t, pc.assignmentScanDone)
}

func TestPushAssignmentFailedStartupDoesNotRegisterScan(t *testing.T) {
	pc, _, _ := newAssignmentTestConsumer(t, 0)
	pc.cli.cancel()
	if err := pc.Start(); err == nil {
		t.Fatal("startup with a canceled client unexpectedly succeeded")
	}
	if !pc.stopping.Load() || pc.assignmentScanDone != nil {
		t.Fatal("failed startup did not stop without registering an assignment scanner")
	}
}

func TestPushAssignmentQueryFailureContinuesOtherTopics(t *testing.T) {
	pc, manager, mq := newAssignmentTestConsumer(t, 0)
	previous := []*v2.Assignment{{MessageQueue: mq}}
	pc.cacheAssignments.Store(MOCK_TOPIC, &previous)
	pq := pc.createProcessQueue(utils.ParseMessageQueue2Str(mq), mq, SUB_ALL).(*defaultProcessQueue)
	other := proto.Clone(mq).(*v2.MessageQueue)
	other.Topic = &v2.Resource{Name: "other-topic"}
	otherPrevious := []*v2.Assignment{{MessageQueue: other}}
	pc.subscriptionExpressions.Store(other.Topic.Name, SUB_ALL)
	pc.cacheAssignments.Store(other.Topic.Name, &otherPrevious)
	otherQueue := pc.createProcessQueue(utils.ParseMessageQueue2Str(other), other, SUB_ALL).(*defaultProcessQueue)
	manager.EXPECT().QueryAssignments(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *v2.Endpoints, request *v2.QueryAssignmentRequest, _ time.Duration) (*v2.QueryAssignmentResponse, error) {
			if request.GetTopic().GetName() == MOCK_TOPIC {
				return nil, errors.New("failed topic")
			}
			return &v2.QueryAssignmentResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
		}).Times(2)
	pc.scanAssignments()
	if pq.dropped.Load() || !otherQueue.dropped.Load() {
		t.Fatal("failed topic was not preserved independently of the successful topic")
	}
}

func TestPushAssignmentRecoversEmptyCache(t *testing.T) {
	pc, manager, mq := newAssignmentTestConsumer(t, 0)
	manager.EXPECT().QueryAssignments(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("unavailable"))
	pc.scanAssignments()
	if _, exists := pc.cacheAssignments.Load(MOCK_TOPIC); exists || pc.getQueueSize() != 0 {
		t.Fatal("failed query changed an initially empty cache")
	}
	manager.EXPECT().QueryAssignments(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&v2.QueryAssignmentResponse{Status: &v2.Status{Code: v2.Code_OK}, Assignments: []*v2.Assignment{{MessageQueue: mq}}}, nil)
	received := make(chan struct{})
	var receiveOnce sync.Once
	manager.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *v2.Endpoints, *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
			receiveOnce.Do(func() { close(received) })
			return &noNewMessageStream{}, nil
		}).AnyTimes()
	pc.scanAssignments()
	waitAssignmentSignal(t, received)
	pc.stopAssignmentScan()
	waitForCondition(t, 5*time.Second, func() bool {
		return pc.inflightRequestCountInterceptor.getInflightReceiveRequestCount() == 0
	}, "the recovery receive to finish")
	if pc.getQueueSize() != 1 {
		t.Fatal("successful retry did not restore the process queue and resume receiving")
	}
	if cached, _ := pc.cacheAssignments.Load(MOCK_TOPIC); utils.IsAssignmentsEmpty(cached.(*[]*v2.Assignment)) {
		t.Fatal("successful retry did not restore the cached assignment")
	}
}
