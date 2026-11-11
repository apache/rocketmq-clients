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
	"testing"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
)

func ackStatusCases() []struct {
	name string
	code v2.Code
} {
	return []struct {
		name string
		code v2.Code
	}{
		{"ok", v2.Code_OK},
		{"invalid receipt handle", v2.Code_INVALID_RECEIPT_HANDLE},
		{"internal error", v2.Code_INTERNAL_ERROR},
	}
}

func ackMessageView() *MessageView {
	return &MessageView{
		messageId:     "msg-1",
		topic:         MOCK_TOPIC,
		ReceiptHandle: "handle-1",
		endpoints:     fakeEndpoints(),
	}
}

func expectAckStatus(t *testing.T, manager *MockClientManager, code v2.Code) {
	t.Helper()
	manager.EXPECT().AckMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&v2.AckMessageResponse{Status: &v2.Status{Code: code, Message: "status from server"}}, nil)
}

// checkAckError requires Ack to report every non-OK status, as the Java client does.
func checkAckError(t *testing.T, code v2.Code, err error) {
	t.Helper()
	if code == v2.Code_OK {
		if err != nil {
			t.Fatalf("expected no error for status OK, got %v", err)
		}
		return
	}
	var rpcErr *ErrRpcStatus
	if !errors.As(err, &rpcErr) {
		t.Fatalf("expected *ErrRpcStatus for status %v, got %v", code, err)
	}
	if rpcErr.GetCode() != int32(code) || rpcErr.GetMessage() != "status from server" {
		t.Fatalf("expected code %d and the status message, got code %d, message %q", int32(code), rpcErr.GetCode(), rpcErr.GetMessage())
	}
}

func TestSimpleConsumerAckReturnsStatusError(t *testing.T) {
	for _, tc := range ackStatusCases() {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := newSimpleConsumer(&Config{
				Endpoint:      fakeAddress,
				ConsumerGroup: MOCK_GROUP,
				Credentials:   &credentials.SessionCredentials{},
			})
			if err != nil {
				t.Fatal(err)
			}
			manager := NewMockClientManager(gomock.NewController(t))
			sc.cli.clientManager = manager
			sc.cli.log = zap.NewNop().Sugar()
			sc.cli.on.Store(true)
			expectAckStatus(t, manager, tc.code)

			checkAckError(t, tc.code, sc.Ack(context.Background(), ackMessageView()))
		})
	}
}

func TestPushConsumerAckReturnsStatusError(t *testing.T) {
	for _, tc := range ackStatusCases() {
		t.Run(tc.name, func(t *testing.T) {
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
			pc.cli.on.Store(true)
			expectAckStatus(t, manager, tc.code)

			checkAckError(t, tc.code, pc.Ack(context.Background(), ackMessageView()))
		})
	}
}
