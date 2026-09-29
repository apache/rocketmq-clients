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
	pc.cli.log = newInternalLogger(newZapLogger(zap.NewNop()))
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
