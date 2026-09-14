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
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	gomock "github.com/golang/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Ordinary send tests use cached routes and a local manager, without starting workers or telemetry.
func newProducerForTest(t *testing.T, maxAttempts int32, delay time.Duration) (*defaultProducer, *MockClientManager) {
	t.Helper()
	producer, err := NewProducer(&Config{
		Endpoint:    fakeAddress,
		Credentials: &credentials.SessionCredentials{},
	}, WithMaxAttempts(maxAttempts), WithTopics(MOCK_TOPIC))
	if err != nil {
		t.Fatal(err)
	}
	p := producer.(*defaultProducer)
	manager := NewMockClientManager(gomock.NewController(t))
	p.cli.clientManager = manager
	p.cli.log = zap.NewNop().Sugar()
	p.cli.inited.Store(true)
	p.pSetting.retryPolicy.Strategy = &v2.RetryPolicy_ExponentialBackoff{
		ExponentialBackoff: &v2.ExponentialBackoff{
			Initial: durationpb.New(delay), Max: durationpb.New(delay), Multiplier: 1,
		},
	}
	p.cli.router.Store(MOCK_TOPIC, []*v2.MessageQueue{{
		Broker: &v2.Broker{Name: "broker", Endpoints: fakeEndpoints()},
		AcceptMessageTypes: []v2.MessageType{
			v2.MessageType_NORMAL, v2.MessageType_DELAY, v2.MessageType_FIFO, v2.MessageType_TRANSACTION,
		},
	}})
	return p, manager
}

func producerSendResponse(code v2.Code) *v2.SendMessageResponse {
	return &v2.SendMessageResponse{
		Status: &v2.Status{Code: code, Message: code.String()},
		Entries: []*v2.SendResultEntry{{
			MessageId: "message-id", TransactionId: "transaction-id", Offset: 42, RecallHandle: "recall-handle",
		}},
	}
}

func TestProducerRetryPolicySnapshot(t *testing.T) {
	p, _ := newProducerForTest(t, 3, time.Millisecond)
	ps := p.pSetting
	previous := ps.GetRetryPolicy()
	previousValue := proto.Clone(previous)
	settings := &v2.Settings{
		PubSub: &v2.Settings_Publishing{Publishing: &v2.Publishing{MaxBodySize: 1024, ValidateMessageType: true}},
		BackoffPolicy: &v2.RetryPolicy{
			MaxAttempts: 5,
			Strategy: &v2.RetryPolicy_ExponentialBackoff{ExponentialBackoff: &v2.ExponentialBackoff{
				Initial: durationpb.New(2 * time.Millisecond), Max: durationpb.New(5 * time.Millisecond), Multiplier: 2,
			}},
		},
	}
	if err := ps.applySettingsCommand(settings); err != nil {
		t.Fatal(err)
	}
	current := ps.GetRetryPolicy()
	if current == previous || !proto.Equal(previous, previousValue) {
		t.Fatal("applying settings mutated the published retry policy snapshot")
	}
	if !proto.Equal(current, settings.BackoffPolicy) || ps.GetRetryPolicy() != current {
		t.Fatal("GetRetryPolicy did not return a stable replacement snapshot")
	}
	currentValue := proto.Clone(current)
	settings.BackoffPolicy.MaxAttempts = 99
	settings.BackoffPolicy.GetExponentialBackoff().Initial.Nanos = 99
	settings.BackoffPolicy.GetExponentialBackoff().Max.Nanos = 99
	if !proto.Equal(current, currentValue) {
		t.Fatal("retry policy aliases the inbound settings command")
	}
	outbound := ps.toProtobuf()
	outbound.BackoffPolicy.MaxAttempts = 99
	outbound.BackoffPolicy.GetExponentialBackoff().Initial.Nanos = 99
	if !proto.Equal(current, currentValue) {
		t.Fatal("outbound settings expose the internal retry policy for mutation")
	}
	// A command without a new exponential strategy retains the previous strategy.
	settings.BackoffPolicy = &v2.RetryPolicy{MaxAttempts: 7}
	if err := ps.applySettingsCommand(settings); err != nil {
		t.Fatal(err)
	}
	if got := ps.GetRetryPolicy(); got.GetMaxAttempts() != 7 || !proto.Equal(got.GetExponentialBackoff(), current.GetExponentialBackoff()) || !proto.Equal(current, currentValue) {
		t.Fatal("partial settings command changed the previous strategy or snapshot")
	}
}

func TestProducerRetryPolicyConcurrentSend(t *testing.T) {
	p, manager := newProducerForTest(t, 3, time.Millisecond)
	const iterations = 100
	manager.EXPECT().SendMessage(gomock.Any(), fakeEndpoints(), gomock.Any(), p.getRequestTimeout()).Return(producerSendResponse(v2.Code_OK), nil).Times(iterations)
	start := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		<-start
		for i := 0; i < iterations; i++ {
			settings := &v2.Settings{
				PubSub: &v2.Settings_Publishing{Publishing: &v2.Publishing{MaxBodySize: 1024, ValidateMessageType: true}},
				BackoffPolicy: &v2.RetryPolicy{
					MaxAttempts: int32(i%3 + 1),
					Strategy: &v2.RetryPolicy_ExponentialBackoff{ExponentialBackoff: &v2.ExponentialBackoff{
						Initial: durationpb.New(time.Millisecond), Max: durationpb.New(time.Second), Multiplier: 2,
					}},
				},
			}
			if err := p.pSetting.applySettingsCommand(settings); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	t.Cleanup(func() {
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("settings writer did not finish")
		}
	})
	close(start)
	for i := 0; i < iterations; i++ {
		if _, err := p.Send(context.Background(), &Message{Topic: MOCK_TOPIC}); err != nil {
			t.Fatal(err)
		}
		if delay := p.getNextAttemptDelay(2); delay <= 0 {
			t.Errorf("invalid delay in retry policy snapshot: %v", delay)
		}
		if _, err := proto.Marshal(p.pSetting.toProtobuf()); err != nil {
			t.Fatal(err)
		}
	}
}
