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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestProducer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		kind       v2.MessageType
		async      bool
		resolution v2.TransactionResolution
	}{
		{name: "normal", kind: v2.MessageType_NORMAL},
		{name: "async", kind: v2.MessageType_NORMAL, async: true},
		{name: "transaction commit", kind: v2.MessageType_TRANSACTION, resolution: v2.TransactionResolution_COMMIT},
		{name: "transaction rollback", kind: v2.MessageType_TRANSACTION, resolution: v2.TransactionResolution_ROLLBACK},
		{name: "fifo", kind: v2.MessageType_FIFO},
		{name: "delay", kind: v2.MessageType_DELAY},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, manager := newProducerForTest(t, 3, 0)
			msg := &Message{Topic: MOCK_TOPIC, Body: []byte("payload")}
			if tc.kind == v2.MessageType_FIFO {
				msg.SetMessageGroup(MOCK_GROUP)
			}
			if tc.kind == v2.MessageType_DELAY {
				msg.SetDelayTimestamp(time.Now().Add(time.Hour))
			}
			manager.EXPECT().SendMessage(gomock.Any(), fakeEndpoints(), gomock.Any(), p.getRequestTimeout()).
				DoAndReturn(func(_ context.Context, _ *v2.Endpoints, req *v2.SendMessageRequest, _ time.Duration) (*v2.SendMessageResponse, error) {
					if len(req.GetMessages()) != 1 {
						t.Errorf("sent %d messages, want 1", len(req.GetMessages()))
					} else {
						wire := req.Messages[0]
						if wire.GetTopic().GetName() != msg.Topic || !bytes.Equal(wire.GetBody(), msg.Body) || wire.GetSystemProperties().GetMessageType() != tc.kind {
							t.Errorf("unexpected sent message: %v", wire)
						}
						if tc.kind == v2.MessageType_FIFO && wire.GetSystemProperties().GetMessageGroup() != MOCK_GROUP {
							t.Error("message group was not preserved")
						}
						if tc.kind == v2.MessageType_DELAY && !wire.GetSystemProperties().GetDeliveryTimestamp().AsTime().Equal(*msg.GetDeliveryTimestamp()) {
							t.Error("delivery timestamp was not preserved")
						}
					}
					return producerSendResponse(v2.Code_OK), nil
				})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var receipts []*SendReceipt
			var err error
			switch {
			case tc.async:
				done := make(chan struct{})
				t.Cleanup(func() {
					cancel()
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("async callback did not finish")
					}
				})
				p.SendAsync(ctx, msg, func(_ context.Context, result []*SendReceipt, sendErr error) {
					receipts, err = result, sendErr
					close(done)
				})
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("async send timed out")
				}
			case tc.kind == v2.MessageType_TRANSACTION:
				manager.EXPECT().EndTransaction(gomock.Any(), fakeEndpoints(), gomock.Any(), p.getRequestTimeout()).
					DoAndReturn(func(_ context.Context, _ *v2.Endpoints, req *v2.EndTransactionRequest, _ time.Duration) (*v2.EndTransactionResponse, error) {
						if req.GetResolution() != tc.resolution || req.GetMessageId() != "message-id" || req.GetTransactionId() != "transaction-id" || req.GetTopic().GetName() != MOCK_TOPIC {
							t.Errorf("unexpected transaction request: %v", req)
						}
						return &v2.EndTransactionResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
					})
				tx := p.BeginTransaction()
				receipts, err = p.SendWithTransaction(ctx, msg, tx)
				if err != nil {
					t.Fatal(err)
				}
				if tc.resolution == v2.TransactionResolution_COMMIT {
					err = tx.Commit()
				} else {
					err = tx.RollBack()
				}
			default:
				receipts, err = p.Send(ctx, msg)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(receipts) != 1 {
				t.Fatalf("received %d receipts, want 1", len(receipts))
			}
			receipt := receipts[0]
			if receipt.MessageID != "message-id" || receipt.TransactionId != "transaction-id" || receipt.Offset != 42 || receipt.RecallHandle != "recall-handle" || !proto.Equal(receipt.Endpoints, fakeEndpoints()) {
				t.Errorf("unexpected receipt: %+v", receipt)
			}
		})
	}
	t.Run("heartbeat", func(t *testing.T) {
		p, manager := newProducerForTest(t, 3, 0)
		manager.EXPECT().HeartBeat(gomock.Any(), fakeEndpoints(), &v2.HeartbeatRequest{ClientType: v2.ClientType_PRODUCER}, p.getRequestTimeout()).
			Return(&v2.HeartbeatResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil)
		if err := p.cli.doHeartbeat(fakeAddress, p.wrapHeartbeatRequest()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProducerThrottlingBackoff(t *testing.T) {
	transportErr := normalizeGrpcError(status.Error(codes.ResourceExhausted, "transport throttled"))
	for _, tc := range []struct {
		name string
		resp *v2.SendMessageResponse
		err  error
	}{
		{name: "protocol", resp: producerSendResponse(v2.Code_TOO_MANY_REQUESTS)},
		{name: "transport", err: transportErr},
		{name: "wrapped transport", err: fmt.Errorf("send: %w", transportErr)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const delay = 20 * time.Millisecond
			p, manager := newProducerForTest(t, 3, delay)
			var lastAttempt time.Time
			var messageID string
			attempt := 0
			manager.EXPECT().SendMessage(gomock.Any(), fakeEndpoints(), gomock.Any(), p.getRequestTimeout()).
				DoAndReturn(func(_ context.Context, _ *v2.Endpoints, req *v2.SendMessageRequest, _ time.Duration) (*v2.SendMessageResponse, error) {
					attempt++
					if attempt > 1 {
						if elapsed := time.Since(lastAttempt); elapsed < delay {
							t.Errorf("attempt %d waited %v, want at least %v", attempt, elapsed, delay)
						}
						if req.Messages[0].GetSystemProperties().GetMessageId() != messageID {
							t.Error("retry changed the message ID")
						}
					}
					messageID = req.Messages[0].GetSystemProperties().GetMessageId()
					lastAttempt = time.Now()
					if attempt < 3 {
						return tc.resp, tc.err
					}
					return producerSendResponse(v2.Code_OK), nil
				}).Times(3)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			receipts, err := p.Send(ctx, &Message{Topic: MOCK_TOPIC})
			if err != nil || len(receipts) != 1 || receipts[0].MessageID != "message-id" {
				t.Fatalf("Send() = (%v, %v), want successful receipt", receipts, err)
			}
		})
	}
}

// Done is observed only when send1 enters its backoff select; no sleeps are needed to gate cancellation.
type producerBackoffContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *producerBackoffContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestProducerCancelDuringBackoff(t *testing.T) {
	for _, transport := range []bool{false, true} {
		t.Run(fmt.Sprintf("transport=%v", transport), func(t *testing.T) {
			p, manager := newProducerForTest(t, 3, 2*time.Second)
			var resp *v2.SendMessageResponse
			var sendErr error
			if transport {
				sendErr = normalizeGrpcError(status.Error(codes.ResourceExhausted, "throttled"))
			} else {
				resp = producerSendResponse(v2.Code_TOO_MANY_REQUESTS)
			}
			manager.EXPECT().SendMessage(gomock.Any(), fakeEndpoints(), gomock.Any(), p.getRequestTimeout()).Return(resp, sendErr).Times(1)
			base, cancel := context.WithCancel(context.Background())
			ctx := &producerBackoffContext{Context: base, waiting: make(chan struct{})}
			result := make(chan error, 1)
			finished := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("send goroutine did not finish")
				}
			})
			go func() {
				defer close(finished)
				receipts, err := p.Send(ctx, &Message{Topic: MOCK_TOPIC})
				if receipts != nil {
					t.Error("canceled send returned receipts")
				}
				result <- err
			}()
			select {
			case <-ctx.waiting:
			case <-time.After(time.Second):
				t.Fatal("send did not enter cancellable backoff")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Send() error = %v, want context.Canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not interrupt backoff")
			}
		})
	}
}

func TestProducerOtherErrorsRetryWithoutBackoff(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *v2.SendMessageResponse
		err  error
	}{
		{name: "ordinary", err: errors.New("send failed")},
		{name: "unavailable", err: normalizeGrpcError(status.Error(codes.Unavailable, "unavailable"))},
		{name: "permission denied", err: normalizeGrpcError(status.Error(codes.PermissionDenied, "denied"))},
		{name: "protocol", resp: producerSendResponse(v2.Code_INTERNAL_ERROR)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, manager := newProducerForTest(t, 2, 200*time.Millisecond)
			gomock.InOrder(
				manager.EXPECT().SendMessage(gomock.Any(), fakeEndpoints(), gomock.Any(), p.getRequestTimeout()).Return(tc.resp, tc.err),
				manager.EXPECT().SendMessage(gomock.Any(), fakeEndpoints(), gomock.Any(), p.getRequestTimeout()).Return(producerSendResponse(v2.Code_OK), nil),
			)
			base, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx := &producerBackoffContext{Context: base, waiting: make(chan struct{})}
			if _, err := p.Send(ctx, &Message{Topic: MOCK_TOPIC}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-ctx.waiting:
				t.Error("non-throttling error entered backoff")
			default:
			}
		})
	}
}

func TestProducerRetryLimitPreservesError(t *testing.T) {
	cause := status.Error(codes.ResourceExhausted, "transport throttled")
	for _, tc := range []struct {
		name string
		resp *v2.SendMessageResponse
		err  error
	}{
		{name: "protocol", resp: producerSendResponse(v2.Code_TOO_MANY_REQUESTS)},
		{name: "transport", err: normalizeGrpcError(cause)},
		{name: "ordinary", err: errors.New("ordinary failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, manager := newProducerForTest(t, 3, 0)
			manager.EXPECT().SendMessage(gomock.Any(), fakeEndpoints(), gomock.Any(), p.getRequestTimeout()).Return(tc.resp, tc.err).Times(3)
			receipts, err := p.Send(context.Background(), &Message{Topic: MOCK_TOPIC})
			if receipts != nil || err == nil {
				t.Fatalf("Send() = (%v, %v), want only an error", receipts, err)
			}
			if tc.err != nil && err != tc.err {
				t.Fatalf("Send() error = %v, want original error %v", err, tc.err)
			}
			if tc.name == "protocol" || tc.name == "transport" {
				rpcErr, ok := AsErrRpcStatus(err)
				if !ok || rpcErr.Code != int32(v2.Code_TOO_MANY_REQUESTS) {
					t.Fatalf("Send() error = %v, want SDK TOO_MANY_REQUESTS", err)
				}
			}
			if tc.name == "transport" && (!errors.Is(err, cause) || status.Code(err) != codes.ResourceExhausted) {
				t.Errorf("transport error lost its original cause: %v", err)
			}
		})
	}
}

func TestProducerCanceledBeforeSend(t *testing.T) {
	p, _ := newProducerForTest(t, 3, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if receipts, err := p.Send(ctx, &Message{Topic: MOCK_TOPIC}); receipts != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Send() = (%v, %v), want context.Canceled without an RPC", receipts, err)
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
