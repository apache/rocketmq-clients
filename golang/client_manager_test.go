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
	"os"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	gomock "github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
)

var MOCK_CLIENT_ID = "mock_client_id"
var MOCK_TOPIC = "mock_topic"
var MOCK_GROUP = "mock_group"
var MOCK_CLIENT *MockClient
var MOCK_RPC_CLIENT *MockRpcClient

func TestMain(m *testing.M) {
	os.Setenv("mq.consoleAppender.enabled", "true")
	ResetLogger()

	ctrl := gomock.NewController(nil)

	MOCK_CLIENT = NewMockClient(ctrl)
	MOCK_CLIENT.EXPECT().GetClientID().Return(MOCK_CLIENT_ID).AnyTimes()

	MOCK_RPC_CLIENT = NewMockRpcClient(ctrl)
	MOCK_RPC_CLIENT.EXPECT().HeartBeat(gomock.Any(), gomock.Any()).Return(&v2.HeartbeatResponse{
		Status: &v2.Status{
			Code: v2.Code_OK,
		},
	}, nil).AnyTimes()
	MOCK_RPC_CLIENT.EXPECT().GracefulStop().Return(nil).AnyTimes()
	MOCK_RPC_CLIENT.EXPECT().GetTarget().Return(fakeAddresss).AnyTimes()
	stubs := gostub.Stub(&NewRpcClient, func(target string, opts ...RpcClientOption) (RpcClient, error) {
		if target == fakeAddresss {
			return MOCK_RPC_CLIENT, nil
		}
		return nil, fmt.Errorf("invalid target=%s", target)
	})
	defer stubs.Reset()

	sugarBaseLogger.Info("begin")
	m.Run()
	sugarBaseLogger.Info("end")
}

func TestCMRegisterClient(t *testing.T) {
	defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	v, ok := defaultClientManagerRegistry.singletonClientManager.(*defaultClientManager).clientTable.Load(MOCK_CLIENT_ID)
	if !ok {
		t.Errorf("test RegisterClient failed")
	}
	exitCli, ok := v.(Client)
	if !ok {
		t.Errorf("test RegisterClient failed")
	}
	if exitCli.GetClientID() != MOCK_CLIENT_ID {
		t.Errorf("test RegisterClient failed")
	}
}

func TestCMUnRegisterClient(t *testing.T) {
	defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)
	if defaultClientManagerRegistry.singletonClientManager != nil {
		t.Errorf("test UnRegisterClient failed")
	}
}

var (
	fakeHost           = "127.0.0.1"
	fakePort     int32 = 80
	fakeAddresss       = fmt.Sprintf("%s:%d", fakeHost, fakePort)
)

func fakeEndpoints() *v2.Endpoints {
	return &v2.Endpoints{
		Addresses: []*v2.Address{
			{
				Host: fakeHost,
				Port: fakePort,
			},
		},
	}
}
func TestCMQueryRoute(t *testing.T) {
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	MOCK_RPC_CLIENT.EXPECT().QueryRoute(gomock.Any(), gomock.Any()).Return(&v2.QueryRouteResponse{
		Status: &v2.Status{
			Code: v2.Code_OK,
		},
	}, nil)
	resp, err := cm.QueryRoute(context.TODO(), fakeEndpoints(), &v2.QueryRouteRequest{}, time.Minute)
	if err != nil {
		t.Error(err)
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		t.Errorf("test QueryRoute failed")
	}
}

func TestCMHeartBeat(t *testing.T) {
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	resp, err := cm.HeartBeat(context.TODO(), fakeEndpoints(), &v2.HeartbeatRequest{}, time.Minute)
	if err != nil {
		t.Error(err)
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		t.Errorf("test HeartBeat failed")
	}
}

func TestCMSendMessage(t *testing.T) {
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	MOCK_RPC_CLIENT.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&v2.SendMessageResponse{
		Status: &v2.Status{
			Code: v2.Code_OK,
		},
	}, nil)
	resp, err := cm.SendMessage(context.TODO(), fakeEndpoints(), &v2.SendMessageRequest{}, time.Minute)
	if err != nil {
		t.Error(err)
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		t.Errorf("test SendMessage failed")
	}
}

func TestCMTelemetry(t *testing.T) {
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	MOCK_RPC_CLIENT.EXPECT().Telemetry(gomock.Any()).Return(nil, nil)
	_, err := cm.Telemetry(context.TODO(), fakeEndpoints(), time.Minute)
	if err != nil {
		t.Error(err)
	}
}

func TestCMEndTransaction(t *testing.T) {
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	MOCK_RPC_CLIENT.EXPECT().EndTransaction(gomock.Any(), gomock.Any()).Return(&v2.EndTransactionResponse{
		Status: &v2.Status{
			Code: v2.Code_OK,
		},
	}, nil)
	resp, err := cm.EndTransaction(context.TODO(), fakeEndpoints(), &v2.EndTransactionRequest{}, time.Minute)
	if err != nil {
		t.Error(err)
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		t.Errorf("test EndTransaction failed")
	}
}

func TestCMNotifyClientTermination(t *testing.T) {
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	MOCK_RPC_CLIENT.EXPECT().NotifyClientTermination(gomock.Any(), gomock.Any()).Return(&v2.NotifyClientTerminationResponse{
		Status: &v2.Status{
			Code: v2.Code_OK,
		},
	}, nil)
	resp, err := cm.NotifyClientTermination(context.TODO(), fakeEndpoints(), &v2.NotifyClientTerminationRequest{}, time.Minute)
	if err != nil {
		t.Error(err)
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		t.Errorf("test NotifyClientTermination failed")
	}
}

func TestCMReceiveMessage(t *testing.T) {
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	MOCK_RPC_CLIENT.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any()).Return(nil, nil)
	_, err := cm.ReceiveMessage(context.TODO(), fakeEndpoints(), &v2.ReceiveMessageRequest{})
	if err != nil {
		t.Error(err)
	}
}

func TestCMAckMessage(t *testing.T) {
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	MOCK_RPC_CLIENT.EXPECT().AckMessage(gomock.Any(), gomock.Any()).Return(&v2.AckMessageResponse{
		Status: &v2.Status{
			Code: v2.Code_OK,
		},
	}, nil)
	resp, err := cm.AckMessage(context.TODO(), fakeEndpoints(), &v2.AckMessageRequest{}, time.Minute)
	if err != nil {
		t.Error(err)
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		t.Errorf("test AckMessage failed")
	}
}

func TestCMClearIdleRpcClients(t *testing.T) {
	stubs := gostub.Stub(&defaultClientManagerOptions, clientManagerOptions{
		RPC_CLIENT_MAX_IDLE_DURATION: time.Second,

		RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY: time.Duration(0),
		RPC_CLIENT_IDLE_CHECK_PERIOD:        time.Duration(100),

		HEART_BEAT_INITIAL_DELAY: time.Hour,
		HEART_BEAT_PERIOD:        time.Hour,

		LOG_STATS_INITIAL_DELAY: time.Hour,
		LOG_STATS_PERIOD:        time.Hour,

		SYNC_SETTINGS_DELAY:  time.Hour,
		SYNC_SETTINGS_PERIOD: time.Hour,
	})
	defer stubs.Reset()

	MOCK_RPC_CLIENT.EXPECT().idleDuration().Return(time.Hour * 24 * 365)
	cm := defaultClientManagerRegistry.RegisterClient(MOCK_CLIENT)
	defer defaultClientManagerRegistry.UnRegisterClient(MOCK_CLIENT)

	cm.HeartBeat(context.TODO(), fakeEndpoints(), &v2.HeartbeatRequest{}, time.Minute)

	startTime := time.Now()
	for len(defaultClientManagerRegistry.singletonClientManager.(*defaultClientManager).rpcClientTable) != 0 {
		if time.Since(startTime) > time.Second*5 {
			t.Errorf("test ClearIdleRpcClients failed")
		}
		time.Sleep(time.Duration(100))
	}
}
