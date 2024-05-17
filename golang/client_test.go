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
	"reflect"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	gomock "github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func BuildCLient(t *testing.T) *defaultClient {
	stubs := gostub.Stub(&defaultClientManagerOptions, clientManagerOptions{
		RPC_CLIENT_MAX_IDLE_DURATION: time.Second,

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
	}, nil)

	endpoints := fmt.Sprintf("%s:%d", fakeHost, fakePort)
	cli, err := NewClientConcrete(&Config{
		Endpoint:    endpoints,
		Credentials: &credentials.SessionCredentials{},
	})
	if err != nil {
		t.Error(err)
	}
	err = cli.startUp()
	if err != nil {
		t.Error(err)
	}

	return cli
}

func GetClientAndDefaultClientSession(t *testing.T) (*defaultClient, *defaultClientSession) {
	cli := BuildCLient(t)
	default_cli_session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Error(err)
	}
	return cli, default_cli_session
}

func PrepareTestLogger(cli *defaultClient) *observer.ObservedLogs {
	observedZapCore, observedLogs := observer.New(zap.InfoLevel)
	observedLogger := zap.New(observedZapCore)
	cli.log = observedLogger.Sugar()

	return observedLogs
}

func TestCLINewClient(t *testing.T) {
	stubs := gostub.Stub(&defaultClientManagerOptions, clientManagerOptions{
		RPC_CLIENT_MAX_IDLE_DURATION: time.Second,

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
	}, nil)

	endpoints := fmt.Sprintf("%s:%d", fakeHost, fakePort)
	cli, err := NewClient(&Config{
		Endpoint:    endpoints,
		Credentials: &credentials.SessionCredentials{},
	})
	if err != nil {
		t.Error(err)
	}
	sugarBaseLogger.Info(cli)
	err = cli.(*defaultClient).startUp()
	if err != nil {
		t.Error(err)
	}
}

func Test_acquire_observer_uninitialized(t *testing.T) {
	// given
	_, default_cli_session := GetClientAndDefaultClientSession(t)

	// when
	observer, acquired_observer := default_cli_session._acquire_observer()

	// then
	if acquired_observer {
		t.Error("Acquired observer even though it is uninitialized")
	}
	if observer != nil {
		t.Error("Observer should be nil")
	}
}

func Test_acquire_observer_initialized(t *testing.T) {
	// given
	_, default_cli_session := GetClientAndDefaultClientSession(t)
	default_cli_session.publish(context.TODO(), &v2.TelemetryCommand{})

	// when
	observer, acquired_observer := default_cli_session._acquire_observer()

	// then
	if !acquired_observer {
		t.Error("Failed to acquire observer even though it is uninitialized")
	}
	if observer == nil {
		t.Error("Observer should be not nil")
	}
}

func Test_execute_server_telemetry_command_fail(t *testing.T) {
	// given
	cli, default_cli_session := GetClientAndDefaultClientSession(t)
	default_cli_session.publish(context.TODO(), &v2.TelemetryCommand{})
	observedLogs := PrepareTestLogger(cli)

	// when
	default_cli_session._execute_server_telemetry_command(&v2.TelemetryCommand{})

	// then
	logs := observedLogs.All()
	messages := make([]string, len(logs))
	for index, log := range logs {
		messages[index] = log.Message
	}
	assert.Contains(t, messages, "telemetryCommand recv err=%!w(*errors.errorString=&{handleTelemetryCommand err = Command is nil})")
}

func Test_execute_server_telemetry_command(t *testing.T) {
	// given
	cli, default_cli_session := GetClientAndDefaultClientSession(t)
	default_cli_session.publish(context.TODO(), &v2.TelemetryCommand{})
	observedLogs := PrepareTestLogger(cli)

	// when
	default_cli_session._execute_server_telemetry_command(&v2.TelemetryCommand{Command: &v2.TelemetryCommand_RecoverOrphanedTransactionCommand{}})

	// then
	logs := observedLogs.All()
	messages := make([]string, len(logs))
	for index, log := range logs {
		messages[index] = log.Message
	}
	assert.Contains(t, messages, "Executed command successfully")
}

func TestRestoreDefaultClientSessionZeroErrors(t *testing.T) {
	// given
	cli := BuildCLient(t)
	default_cli_session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Error(err)
	}
	default_cli_session.publish(context.TODO(), &v2.TelemetryCommand{})
	observedLogs := PrepareTestLogger(cli)
	default_cli_session.observer = &MOCK_MessagingService_TelemetryClient{
		recv_error_count: 0,
		cli:              cli,
	}
	default_cli_session.recoveryWaitTime = time.Second
	cli.settings = &simpleConsumerSettings{}

	// when
	time.Sleep(3 * time.Second)

	// then
	sugarBaseLogger.Info(observedLogs.All())
	commandExecutionLog := observedLogs.All()[:2]
	assert.Equal(t, "Executed command successfully", commandExecutionLog[0].Message)
	assert.Equal(t, "Executed command successfully", commandExecutionLog[1].Message)
}

func TestRestoreDefaultClientSessionOneError(t *testing.T) {
	// given
	cli := BuildCLient(t)
	default_cli_session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Error(err)
	}
	default_cli_session.publish(context.TODO(), &v2.TelemetryCommand{})
	observedLogs := PrepareTestLogger(cli)
	default_cli_session.observer = &MOCK_MessagingService_TelemetryClient{
		recv_error_count: 1,
		cli:              cli,
	}
	default_cli_session.recoveryWaitTime = time.Second
	cli.settings = &simpleConsumerSettings{}

	// when
	time.Sleep(3 * time.Second)

	// then
	sugarBaseLogger.Info(observedLogs.All())
	commandExecutionLog := observedLogs.All()[:3]
	assert.Equal(t, "Encountered error while receiving TelemetryCommand, trying to recover", commandExecutionLog[0].Message)
	assert.Equal(t, "Managed to recover, executing message", commandExecutionLog[1].Message)
	assert.Equal(t, "Executed command successfully", commandExecutionLog[2].Message)
}

func TestRestoreDefaultClientSessionTwoErrors(t *testing.T) {
	// given
	cli := BuildCLient(t)
	default_cli_session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Error(err)
	}
	default_cli_session.publish(context.TODO(), &v2.TelemetryCommand{})
	observedLogs := PrepareTestLogger(cli)
	default_cli_session.observer = &MOCK_MessagingService_TelemetryClient{
		recv_error_count: 2,
		cli:              cli,
	}
	default_cli_session.recoveryWaitTime = time.Second
	cli.settings = &simpleConsumerSettings{}

	// when
	time.Sleep(3 * time.Second)

	// then
	sugarBaseLogger.Info(observedLogs.All())
	commandExecutionLog := observedLogs.All()[:2]
	assert.Equal(t, "Encountered error while receiving TelemetryCommand, trying to recover", commandExecutionLog[0].Message)
	assert.Equal(t, "Failed to recover, err=EOF", commandExecutionLog[1].Message)
}

func Test_routeEqual(t *testing.T) {
	oldMq := &v2.MessageQueue{
		Topic: &v2.Resource{
			Name:              "topic-test",
			ResourceNamespace: "ns-test",
		},
		Id:         0,
		Permission: v2.Permission_READ_WRITE,
		Broker: &v2.Broker{
			Name:      "broker-test",
			Id:        0,
			Endpoints: fakeEndpoints(),
		},
		AcceptMessageTypes: []v2.MessageType{
			v2.MessageType_NORMAL,
		},
	}
	newMq := &v2.MessageQueue{
		Topic: &v2.Resource{
			Name:              "topic-test",
			ResourceNamespace: "ns-test",
		},
		Id:         0,
		Permission: v2.Permission_READ_WRITE,
		Broker: &v2.Broker{
			Name:      "broker-test",
			Id:        0,
			Endpoints: fakeEndpoints(),
		},
		AcceptMessageTypes: []v2.MessageType{
			v2.MessageType_NORMAL,
		},
	}

	newMq.ProtoReflect() // message internal field value will be changed

	oldRoute := []*v2.MessageQueue{oldMq}
	newRoute := []*v2.MessageQueue{newMq}

	assert.Equal(t, false, reflect.DeepEqual(oldRoute, newRoute))
	assert.Equal(t, true, routeEqual(oldRoute, newRoute))
	assert.Equal(t, true, routeEqual(nil, nil))
	assert.Equal(t, false, routeEqual(nil, newRoute))
	assert.Equal(t, false, routeEqual(oldRoute, nil))
	assert.Equal(t, true, routeEqual(nil, []*v2.MessageQueue{}))
}
