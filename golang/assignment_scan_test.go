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
	"fmt"
	"testing"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
)

// newAssignmentScanTestProcessQueue builds a push consumer whose RPCs are served by
// a mock client manager, plus one process queue to drive the settlement chains
// (ack, change invisible duration, forward to DLQ) against it.
func newAssignmentScanTestProcessQueue(t *testing.T) (*defaultPushConsumer, *defaultProcessQueue, *MockClientManager) {
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

func TestScanAssignmentsKeepsQueuesWhenScanFails(t *testing.T) {
	pc, dpq, manager := newAssignmentScanTestProcessQueue(t)
	pc.processQueueTable.Store(dpq.mqstr, []interface{}{dpq.mq, dpq})
	assignments := &[]*v2.Assignment{{MessageQueue: dpq.mq}}
	pc.cacheAssignments.Store(MOCK_TOPIC, assignments)
	manager.EXPECT().QueryAssignments(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("proxy down")).AnyTimes()

	pc.scanAssignments()

	if _, ok := pc.processQueueTable.Load(dpq.mqstr); !ok {
		t.Fatalf("process queue was dropped after a failed assignment scan; an RPC error must not be treated as empty assignments")
	}
	if cached, _ := pc.cacheAssignments.Load(MOCK_TOPIC); cached != assignments {
		t.Fatalf("cached assignments were replaced after a failed scan")
	}
}
