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
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
)

func newSimpleConsumerForTest(t *testing.T) (*defaultSimpleConsumer, *MockClientManager) {
	t.Helper()
	consumer, err := NewSimpleConsumer(&Config{
		Endpoint:      fakeAddress,
		ConsumerGroup: MOCK_GROUP,
	})
	if err != nil {
		t.Fatal(err)
	}
	sc := consumer.(*defaultSimpleConsumer)
	manager := NewMockClientManager(gomock.NewController(t))
	sc.cli.clientManager = manager
	sc.cli.log = zap.NewNop().Sugar()
	return sc, manager
}

// A non-OK status must surface as an error, otherwise the caller believes the
// message was acknowledged while the broker still holds it invisible.
func TestSimpleConsumerAckReportsNonOkStatus(t *testing.T) {
	sc, manager := newSimpleConsumerForTest(t)

	manager.EXPECT().AckMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.AckMessageRequest, time.Duration) (*v2.AckMessageResponse, error) {
			return &v2.AckMessageResponse{
				Status: &v2.Status{Code: v2.Code_INVALID_RECEIPT_HANDLE, Message: v2.Code_INVALID_RECEIPT_HANDLE.String()},
			}, nil
		}).Times(1)

	err := sc.Ack(context.Background(), testMessageView())
	var rpcErr *ErrRpcStatus
	if !errors.As(err, &rpcErr) {
		t.Fatalf("expected the non-OK ack status to surface as an ErrRpcStatus, got %v", err)
	}
	if rpcErr.GetCode() != int32(v2.Code_INVALID_RECEIPT_HANDLE) {
		t.Fatalf("expected code %d, got %d", int32(v2.Code_INVALID_RECEIPT_HANDLE), rpcErr.GetCode())
	}
}

// A failed change-invisible-duration request carries no new receipt handle;
// adopting the empty one from the response would make every following call fail.
func TestSimpleConsumerChangeInvisibleDurationKeepsReceiptHandleOnError(t *testing.T) {
	sc, manager := newSimpleConsumerForTest(t)

	manager.EXPECT().ChangeInvisibleDuration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *v2.Endpoints, *v2.ChangeInvisibleDurationRequest, time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
			return &v2.ChangeInvisibleDurationResponse{
				Status:        &v2.Status{Code: v2.Code_INTERNAL_SERVER_ERROR, Message: "boom"},
				ReceiptHandle: "",
			}, nil
		}).Times(1)

	mv := testMessageView()
	if err := sc.ChangeInvisibleDuration(mv, time.Second); err == nil {
		t.Fatal("expected the non-OK status to surface as an error")
	}
	if mv.ReceiptHandle != "receipt-handle-0" {
		t.Fatalf("a failed request must not overwrite the receipt handle, got %q", mv.ReceiptHandle)
	}
}
