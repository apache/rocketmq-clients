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
	"io"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	gomock "github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"google.golang.org/grpc/metadata"
)

func TestProducer(t *testing.T) {
	stubs := gostub.Stub(&defaultClientManagerOptions, clientManagerOptions{
		RPC_CLIENT_MAX_IDLE_DURATION: time.Hour,

		RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY: time.Hour,
		RPC_CLIENT_IDLE_CHECK_PERIOD:        time.Hour,

		HEART_BEAT_INITIAL_DELAY: time.Hour,
		HEART_BEAT_PERIOD:        time.Hour,

		LOG_STATS_INITIAL_DELAY: time.Hour,
		LOG_STATS_PERIOD:        time.Hour,

		SYNC_SETTINGS_DELAY:  time.Hour,
		SYNC_SETTINGS_PERIOD: time.Hour,
	})
	defer stubs.Reset()

	endpoints := fmt.Sprintf("%s:%d", fakeHost, fakePort)
	p, err := NewProducer(&Config{
		Endpoint:    endpoints,
		Credentials: &credentials.SessionCredentials{},
	})
	if err != nil {
		t.Error(err)
	}
	MOCK_RPC_CLIENT.EXPECT().QueryRoute(gomock.Any(), gomock.Any()).Return(&v2.QueryRouteResponse{
		Status: &v2.Status{
			Code: v2.Code_OK,
		},
		MessageQueues: []*v2.MessageQueue{{
			Broker: &v2.Broker{
				Endpoints: fakeEndpoints(),
			},
			AcceptMessageTypes: []v2.MessageType{
				v2.MessageType_NORMAL,
				v2.MessageType_DELAY,
				v2.MessageType_FIFO,
				v2.MessageType_TRANSACTION,
			},
		}},
	}, nil)
	err = p.Start()
	if err != nil {
		t.Error(err)
	}

	msg := &Message{
		Topic: MOCK_TOPIC,
		Body:  []byte{},
	}
	t.Run("send normal message", func(t *testing.T) {
		MOCK_RPC_CLIENT.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&v2.SendMessageResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
			Entries: []*v2.SendResultEntry{{}},
		}, nil)

		_, err := p.Send(context.TODO(), msg)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("send async", func(t *testing.T) {
		MOCK_RPC_CLIENT.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&v2.SendMessageResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
			Entries: []*v2.SendResultEntry{{}},
		}, nil)

		done := make(chan bool)
		p.SendAsync(context.TODO(), msg, func(ctx context.Context, sr []*SendReceipt, err error) {
			if err != nil {
				t.Error(err)
			}
			done <- true
		})
		<-done
	})
	t.Run("send transaction message and commit", func(t *testing.T) {
		MOCK_RPC_CLIENT.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&v2.SendMessageResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
			Entries: []*v2.SendResultEntry{{}},
		}, nil)
		MOCK_RPC_CLIENT.EXPECT().EndTransaction(gomock.Any(), gomock.Any()).Return(&v2.EndTransactionResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
		}, nil)

		transaction := p.BeginTransaction()
		_, err := p.SendWithTransaction(context.TODO(), msg, transaction)
		if err != nil {
			t.Error(err)
		}
		err = transaction.Commit()
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("send transaction message and rollback", func(t *testing.T) {
		MOCK_RPC_CLIENT.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&v2.SendMessageResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
			Entries: []*v2.SendResultEntry{{}},
		}, nil)
		MOCK_RPC_CLIENT.EXPECT().EndTransaction(gomock.Any(), gomock.Any()).Return(&v2.EndTransactionResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
		}, nil)

		transaction := p.BeginTransaction()
		_, err := p.SendWithTransaction(context.TODO(), msg, transaction)
		if err != nil {
			t.Error(err)
		}
		err = transaction.RollBack()
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("send fifo msg", func(t *testing.T) {
		MOCK_RPC_CLIENT.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&v2.SendMessageResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
			Entries: []*v2.SendResultEntry{{}},
		}, nil)
		msg.SetMessageGroup(MOCK_GROUP)
		defer func() { msg.messageGroup = nil }()
		_, err := p.Send(context.TODO(), msg)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("send delay msg", func(t *testing.T) {
		MOCK_RPC_CLIENT.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&v2.SendMessageResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
			Entries: []*v2.SendResultEntry{{}},
		}, nil)
		msg.SetDelayTimestamp(time.Now().Add(time.Hour))
		defer func() { msg.deliveryTimestamp = nil }()
		_, err := p.Send(context.TODO(), msg)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("send message retry", func(t *testing.T) {
		codes := []v2.Code{v2.Code_TOO_MANY_REQUESTS, v2.Code_TOO_MANY_REQUESTS, v2.Code_OK}
		retryTimes := 0
		MOCK_RPC_CLIENT.EXPECT().SendMessage(gomock.Any(), gomock.Any()).DoAndReturn(func(p1, p2 interface{}) (*v2.SendMessageResponse, error) {
			retryTimes++
			return &v2.SendMessageResponse{
				Status: &v2.Status{
					Code: codes[retryTimes-1],
				},
				Entries: []*v2.SendResultEntry{{}},
			}, nil
		}).Times(3)
		defer func() { msg.deliveryTimestamp = nil }()
		_, err := p.Send(context.TODO(), msg)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("syncsettings", func(t *testing.T) {
		mt := &MOCK_MessagingService_TelemetryClient{
			trace: make([]string, 0),
		}
		MOCK_RPC_CLIENT.EXPECT().Telemetry(gomock.Any()).Return(mt, nil)
		p.(*defaultProducer).cli.clientManager.(*defaultClientManager).syncSettings()
		for {
			time.Sleep(time.Duration(100))
			if len(mt.trace) >= 3 && mt.trace[0] == "send" && mt.trace[1] == "recv" && mt.trace[2] == "closesend" {
				break
			}
		}
	})
	t.Run("do heartbeat", func(t *testing.T) {
		err := p.(*defaultProducer).cli.doHeartbeat(endpoints, nil)
		if err != nil {
			t.Error(err)
		}
	})
}

type MOCK_MessagingService_TelemetryClient struct {
	trace []string
}

// CloseSend implements v2.MessagingService_TelemetryClient
func (mt *MOCK_MessagingService_TelemetryClient) CloseSend() error {
	mt.trace = append(mt.trace, "closesend")
	return nil
}

// Context implements v2.MessagingService_TelemetryClient
func (mt *MOCK_MessagingService_TelemetryClient) Context() context.Context {
	mt.trace = append(mt.trace, "context")
	return nil
}

// Header implements v2.MessagingService_TelemetryClient
func (mt *MOCK_MessagingService_TelemetryClient) Header() (metadata.MD, error) {
	mt.trace = append(mt.trace, "header")
	return nil, nil
}

// RecvMsg implements v2.MessagingService_TelemetryClient
func (mt *MOCK_MessagingService_TelemetryClient) RecvMsg(m interface{}) error {
	mt.trace = append(mt.trace, "recvmsg")
	return nil
}

// SendMsg implements v2.MessagingService_TelemetryClient
func (mt *MOCK_MessagingService_TelemetryClient) SendMsg(m interface{}) error {
	mt.trace = append(mt.trace, "sendmsg")
	return nil
}

// Trailer implements v2.MessagingService_TelemetryClient
func (mt *MOCK_MessagingService_TelemetryClient) Trailer() metadata.MD {
	mt.trace = append(mt.trace, "trailer")
	return nil
}

// Recv implements v2.MessagingService_TelemetryClient
func (mt *MOCK_MessagingService_TelemetryClient) Recv() (*v2.TelemetryCommand, error) {
	mt.trace = append(mt.trace, "recv")
	return nil, io.EOF
}

// Send implements v2.MessagingService_TelemetryClient
func (mt *MOCK_MessagingService_TelemetryClient) Send(*v2.TelemetryCommand) error {
	mt.trace = append(mt.trace, "send")
	return nil
}

var _ = v2.MessagingService_TelemetryClient(&MOCK_MessagingService_TelemetryClient{})
