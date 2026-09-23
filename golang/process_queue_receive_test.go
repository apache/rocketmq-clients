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
	"io"
	"strings"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProcessQueueReceiveCompletesInflightRequest(t *testing.T) {
	tests := []struct {
		name       string
		code       v2.Code
		receiveErr error
		wantStatus MessageHookPointsStatus
	}{
		{name: "no new message", code: v2.Code_MESSAGE_NOT_FOUND, wantStatus: MessageHookPointsStatus_OK},
		{name: "successful receive", code: v2.Code_OK, wantStatus: MessageHookPointsStatus_OK},
		{name: "server error", code: v2.Code_INTERNAL_SERVER_ERROR, wantStatus: MessageHookPointsStatus_ERROR},
		{name: "receive timeout", receiveErr: status.Error(codes.DeadlineExceeded, "receive timed out"), wantStatus: MessageHookPointsStatus_ERROR},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc, err := newPushConsumer(&Config{Endpoint: fakeAddress, ConsumerGroup: "test-group"},
				WithPushSubscriptionExpressions(map[string]*FilterExpression{"test-topic": NewFilterExpression("*")}),
				WithPushMessageListener(&FuncMessageListener{Consume: func(*MessageView) ConsumerResult { return SUCCESS }}),
			)
			require.NoError(t, err)

			core, logs := observer.New(zap.DebugLevel)
			nextReceive := make(chan struct{}, 1)
			pc.cli.log = zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
				if strings.HasPrefix(entry.Message, "Process queue has been dropped, no longer receive message") {
					nextReceive <- struct{}{}
				}
				return nil
			})).Sugar()
			hooks := &processQueueReceiveHooks{}
			pc.cli.registerMessageInterceptor(hooks)
			manager := NewMockClientManager(gomock.NewController(t))
			pc.cli.clientManager = manager
			manager.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
					if tt.receiveErr != nil {
						return nil, tt.receiveErr
					}
					return &processQueueReceiveStream{code: tt.code}, nil
				}).Times(3)

			pq := &defaultProcessQueue{
				consumer:         pc,
				mq:               &v2.MessageQueue{Topic: &v2.Resource{Name: "test-topic"}, Broker: &v2.Broker{Endpoints: fakeEndpoints()}},
				filterExpression: NewFilterExpression("*"),
			}
			// Prevent automatic polling after each manually started receive. Reaching
			// the dropped-queue log also confirms its completion path has finished.
			pq.dropped.Store(true)
			for attempt := 1; attempt <= 3; attempt++ {
				pq.receiveMessageImmediatelyWithAttemptId("test-attempt")
				select {
				case <-nextReceive:
				case <-time.After(5 * time.Second):
					t.Fatal("receive completion did not reach the next poll")
				}
				assert.Zero(t, pc.inflightRequestCountInterceptor.getInflightReceiveRequestCount(), "after receive %d", attempt)
				assert.Len(t, hooks.statuses, attempt, "each receive must invoke doAfter exactly once")
			}
			assert.Equal(t, []MessageHookPointsStatus{tt.wantStatus, tt.wantStatus, tt.wantStatus}, hooks.statuses)

			// Exercise the same wait used by GracefulStop, with a short upper bound
			// so a leaked counter fails quickly without timing-based assertions.
			pc.pcSettings.requestTimeout = time.Millisecond
			pc.pcSettings.longPollingTimeout = time.Millisecond
			require.NoError(t, pc.waitingReceiveRequestFinished())
			assert.Zero(t, logs.FilterMessageSnippet("Timeout waiting for all inflight receive requests").Len())
			assert.Equal(t, 1, logs.FilterMessageSnippet("All inflight receive requests have been finished").Len())
		})
	}
}

type processQueueReceiveStream struct {
	v2.MessagingService_ReceiveMessageClient
	code v2.Code
	sent bool
}

func (s *processQueueReceiveStream) Recv() (*v2.ReceiveMessageResponse, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	return &v2.ReceiveMessageResponse{Content: &v2.ReceiveMessageResponse_Status{Status: &v2.Status{Code: s.code}}}, nil
}

type processQueueReceiveHooks struct {
	statuses []MessageHookPointsStatus
}

func (h *processQueueReceiveHooks) doBefore(MessageHookPoints, []*MessageCommon) error {
	return nil
}

func (h *processQueueReceiveHooks) doAfter(point MessageHookPoints, _ []*MessageCommon, _ time.Duration, status MessageHookPointsStatus) error {
	if point == MessageHookPoints_RECEIVE {
		h.statuses = append(h.statuses, status)
	}
	return nil
}
