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
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io"
	"testing"
	"time"
)

const (
	Topic         = "xxxxxx"
	ConsumerGroup = "xxxxxx"
	AccessKey     = "xxxxxx"
	SecretKey     = "xxxxxx"
)

var (
	// maximum waiting time for receive func
	awaitDuration = time.Minute
	// invisibleDuration should > 20s
	invisibleDuration = time.Second * 20
	// messageViewCacheSize the maximum received message in the cache
	messageViewCacheSize = 10
	// receive messages in a loop
)

type mockReceiveMessageClient struct {
	grpc.ClientStream
}

var sendType = 0

func (x *mockReceiveMessageClient) Recv() (*v2.ReceiveMessageResponse, error) {
	m := new(v2.ReceiveMessageResponse)
	tmp := "test"
	m.Content = &v2.ReceiveMessageResponse_Message{
		Message: &v2.Message{
			Topic: &v2.Resource{
				Name: "test",
			},
			Body: []byte{},
			SystemProperties: &v2.SystemProperties{
				MessageGroup: &tmp,
				BodyDigest: &v2.Digest{
					Type: v2.DigestType_CRC32,
				},
				BodyEncoding: v2.Encoding_IDENTITY,
			},
		},
	}

	if sendType == 0 {
		sendType++
		return m, nil
	}
	m.Content = &v2.ReceiveMessageResponse_Status{
		Status: &v2.Status{Code: v2.Code_OK},
	}
	if sendType == 1 {
		sendType++
		return m, nil
	}

	sendType = 0
	return m, io.EOF
}

var globalPC *defaultPushConsumer

func BuildPushConsumer(t *testing.T) *defaultPushConsumer {
	MOCK_RPC_CLIENT.EXPECT().Telemetry(gomock.Any()).Return(&MOCK_MessagingService_TelemetryClient{
		trace: make([]string, 0),
	}, nil)

	pc, err := NewPushConsumer(&Config{
		Endpoint:      fakeAddress,
		ConsumerGroup: ConsumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	},
		&FuncPushConsumerOption{
			FuncConsumerOption: WithAwaitDuration(awaitDuration),
		},
		&FuncPushConsumerOption{
			FuncConsumerOption: WithSubscriptionExpressions(map[string]*FilterExpression{
				Topic: SUB_ALL,
			}),
		},
		WithInvisibleDuration(invisibleDuration),
		WithMessageViewCacheSize(messageViewCacheSize),
	)
	if err != nil {
		t.Error(err)
	}

	return pc.(*defaultPushConsumer)
}

func GetPushConsumer(t *testing.T) *defaultPushConsumer {
	return BuildPushConsumer(t)
}

func TestDefaultPushConsumer_GetGroupName(t *testing.T) {
	pc := GetPushConsumer(t)

	groupName := pc.GetGroupName()
	assert.Equal(t, groupName, "xxxxxx")
}

func TestDefaultPushConsumer_GracefulStop(t *testing.T) {
	pc := GetPushConsumer(t)
	ctrl := gomock.NewController(t)
	pc.cli.clientManager = NewMockClientManager(ctrl)
	pc.cli.clientManager.(*MockClientManager).EXPECT().UnRegisterClient(gomock.Any()).Return().AnyTimes()

	err := pc.GracefulStop()
	if err != nil {
		t.Error(err)
	}
}

func TestDefaultPushConsumer_Start(t *testing.T) {
	stubs := gostub.Stub(&NewSubscriptionLoadBalancer, func(messageQueues []*v2.MessageQueue) (SubscriptionLoadBalancer, error) {
		return &subscriptionLoadBalancer{
			messageQueues: []*v2.MessageQueue{
				{
					Broker: &v2.Broker{
						Endpoints: fakeEndpoints(),
					},
				},
			},
		}, nil
	})

	defer stubs.Reset()

	pc := GetPushConsumer(t)
	ctrl := gomock.NewController(t)
	pc.cli.clientManager = NewMockClientManager(ctrl)

	pc.cli.clientManager.(*MockClientManager).EXPECT().
		ReceiveMessage(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockReceiveMessageClient{&MOCK_MessagingService_TelemetryClient{}}, nil).AnyTimes()
	pc.cli.clientManager.(*MockClientManager).EXPECT().
		QueryRoute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&v2.QueryRouteResponse{
			Status:        &v2.Status{Code: v2.Code_OK},
			MessageQueues: []*v2.MessageQueue{{Id: 1}},
		}, nil).AnyTimes()
	MOCK_RPC_CLIENT.EXPECT().
		QueryRoute(gomock.Any(), gomock.Any()).
		Return(&v2.QueryRouteResponse{
			Status:        &v2.Status{Code: v2.Code_OK},
			MessageQueues: []*v2.MessageQueue{{Id: 1}},
		}, nil).AnyTimes()
	MOCK_RPC_CLIENT.EXPECT().
		ReceiveMessage(gomock.Any(), gomock.Any()).
		Return(&mockReceiveMessageClient{&MOCK_MessagingService_TelemetryClient{}}, nil).AnyTimes()

	err := pc.Start()
	time.Sleep(time.Second * 10)
	if err != nil {
		t.Error(err)
	}
}

func TestDefaultPushConsumer_Subscribe(t *testing.T) {
	pc := GetPushConsumer(t)
	ctrl := gomock.NewController(t)
	pc.cli.clientManager = NewMockClientManager(ctrl)

	pc.cli.clientManager.(*MockClientManager).EXPECT().
		QueryRoute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&v2.QueryRouteResponse{
			Status:        &v2.Status{Code: v2.Code_OK},
			MessageQueues: []*v2.MessageQueue{{Id: 1}},
		}, nil).AnyTimes()

	err := pc.Subscribe("", &FilterExpression{}, func(context.Context, *MessageView) (ConsumeResult, error) {
		return 0, nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestDefaultPushConsumer_Unsubscribe(t *testing.T) {
	pc := GetPushConsumer(t)

	err := pc.Unsubscribe("")
	if err != nil {
		t.Error(err)
	}
}

func TestDefaultPushConsumer_Ack(t *testing.T) {
	pc := GetPushConsumer(t)
	ctrl := gomock.NewController(t)
	pc.cli.clientManager = NewMockClientManager(ctrl)

	pc.cli.clientManager.(*MockClientManager).EXPECT().
		AckMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).AnyTimes()

	err := pc.Ack(context.TODO(), &MessageView{endpoints: fakeEndpoints()})
	if err != nil {
		t.Error(err)
	}
}

func TestDefaultPushConsumer_Receive(t *testing.T) {
	pc := GetPushConsumer(t)
	ctrl := gomock.NewController(t)
	pc.cli.clientManager = NewMockClientManager(ctrl)

	pc.cli.clientManager.(*MockClientManager).EXPECT().
		ReceiveMessage(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockReceiveMessageClient{&MOCK_MessagingService_TelemetryClient{}}, nil).AnyTimes()
	pc.cli.clientManager.(*MockClientManager).EXPECT().
		QueryRoute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&v2.QueryRouteResponse{
			Status:        &v2.Status{Code: v2.Code_OK},
			MessageQueues: []*v2.MessageQueue{{Id: 1}},
		}, nil).AnyTimes()

	ctx := context.TODO()
	err := pc.Receive(ctx, pc.invisibleDuration)
	if err != nil && (err.Error() != "[error] CODE=DEADLINE_EXCEEDED" || err.Error() != io.EOF.Error()) {
		t.Error(err)
	}
}

func TestDefaultPushConsumer_ChangeInvisibleDuration(t *testing.T) {
	pc := GetPushConsumer(t)
	ctrl := gomock.NewController(t)
	pc.cli.clientManager = NewMockClientManager(ctrl)

	pc.cli.clientManager.(*MockClientManager).EXPECT().
		ChangeInvisibleDuration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&v2.ChangeInvisibleDurationResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil).AnyTimes()

	err := pc.ChangeInvisibleDuration(&MessageView{endpoints: fakeEndpoints()}, pc.invisibleDuration)
	if err != nil {
		t.Error(err)
	}
}

func TestDefaultPushConsumer_ChangeInvisibleDurationAsync(t *testing.T) {
	pc := GetPushConsumer(t)
	ctrl := gomock.NewController(t)
	pc.cli.clientManager = NewMockClientManager(ctrl)

	pc.cli.clientManager.(*MockClientManager).EXPECT().
		ChangeInvisibleDuration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&v2.ChangeInvisibleDurationResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil).AnyTimes()

	pc.ChangeInvisibleDurationAsync(&MessageView{endpoints: fakeEndpoints()}, pc.invisibleDuration)
}
