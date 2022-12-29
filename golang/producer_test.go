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
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	gomock "github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
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

	stubs2 := gostub.Stub(&NewRpcClient, func(target string, opts ...RpcClientOption) (RpcClient, error) {
		if target == fakeAddress {
			return MOCK_RPC_CLIENT, nil
		}
		return nil, fmt.Errorf("invalid target=%s", target)
	})

	defer func() {
		stubs.Reset()
		stubs2.Reset()
	}()

	MOCK_RPC_CLIENT.EXPECT().Telemetry(gomock.Any()).Return(&MOCK_MessagingService_TelemetryClient{
		trace: make([]string, 0),
	}, nil).AnyTimes()

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
	}, nil).AnyTimes()
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
		}, nil).AnyTimes()

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
		}, nil).AnyTimes()

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
		}, nil).AnyTimes()
		MOCK_RPC_CLIENT.EXPECT().EndTransaction(gomock.Any(), gomock.Any()).Return(&v2.EndTransactionResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
		}, nil).AnyTimes()

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
		}, nil).AnyTimes()
		MOCK_RPC_CLIENT.EXPECT().EndTransaction(gomock.Any(), gomock.Any()).Return(&v2.EndTransactionResponse{
			Status: &v2.Status{
				Code: v2.Code_OK,
			},
		}, nil).AnyTimes()

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
		}, nil).AnyTimes()
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
		}, nil).AnyTimes()
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
	t.Run("do heartbeat", func(t *testing.T) {
		err := p.(*defaultProducer).cli.doHeartbeat(endpoints, nil)
		if err != nil {
			t.Error(err)
		}
	})
}
