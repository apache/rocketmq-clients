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
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// fakeTelemetryStream is a scriptable in-package telemetry stream. Recv drains
// a scripted queue and then blocks until the stream context ends, mirroring a
// healthy long-lived stream; Send can be gated to reproduce a writer stuck on
// an unusable connection.
type fakeTelemetryStream struct {
	grpc.ClientStream
	ctx         context.Context
	mu          sync.Mutex
	sends       []*v2.TelemetryCommand
	closed      bool
	recvQueue   []fakeRecvResult
	sendGate    chan struct{}
	sendEntered chan struct{}
}

type fakeRecvResult struct {
	command *v2.TelemetryCommand
	err     error
}

func newFakeTelemetryStream(ctx context.Context, recvQueue ...fakeRecvResult) *fakeTelemetryStream {
	return &fakeTelemetryStream{ctx: ctx, recvQueue: recvQueue}
}

func (s *fakeTelemetryStream) Context() context.Context { return s.ctx }

func (s *fakeTelemetryStream) Send(command *v2.TelemetryCommand) error {
	s.mu.Lock()
	entered, gate := s.sendEntered, s.sendGate
	if entered != nil {
		close(entered)
		s.sendEntered = nil
	}
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("telemetry stream closed")
	}
	s.sends = append(s.sends, command)
	return nil
}

func (s *fakeTelemetryStream) Recv() (*v2.TelemetryCommand, error) {
	for {
		s.mu.Lock()
		if len(s.recvQueue) > 0 {
			next := s.recvQueue[0]
			s.recvQueue = s.recvQueue[1:]
			s.mu.Unlock()
			return next.command, next.err
		}
		s.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (s *fakeTelemetryStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeTelemetryStream) settingsSends() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, command := range s.sends {
		if command.GetSettings() != nil {
			count++
		}
	}
	return count
}

func (s *fakeTelemetryStream) sendCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sends)
}

func (s *fakeTelemetryStream) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// sendBlocked reports whether a Send has entered and is parked on the gate.
func (s *fakeTelemetryStream) sendBlocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendGate != nil && s.sendEntered == nil
}

func (s *fakeTelemetryStream) gateSends() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendEntered == nil {
		s.sendEntered = make(chan struct{})
	}
	if s.sendGate == nil {
		s.sendGate = make(chan struct{})
	}
	return s.sendEntered
}

// streamRecorder hands stream instances created on the supervisor goroutine to
// the test goroutine without a data race.
type streamRecorder struct {
	mu      sync.Mutex
	streams []*fakeTelemetryStream
}

func (r *streamRecorder) add(s *fakeTelemetryStream) {
	r.mu.Lock()
	r.streams = append(r.streams, s)
	r.mu.Unlock()
}

func (r *streamRecorder) at(i int) *fakeTelemetryStream {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i < len(r.streams) {
		return r.streams[i]
	}
	return nil
}

func (r *streamRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams)
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func subscriptionSettingsCommand() *v2.TelemetryCommand {
	return &v2.TelemetryCommand{
		Command: &v2.TelemetryCommand_Settings{Settings: &v2.Settings{
			PubSub: &v2.Settings_Subscription{Subscription: &v2.Subscription{}},
		}},
	}
}

// BuildCLient constructs a client without background managers or goroutines.
func BuildCLient(t *testing.T) *defaultClient {
	t.Helper()
	cli, err := NewClientConcrete(&Config{
		Endpoint:    fmt.Sprintf("%s:%d", fakeHost, fakePort),
		Credentials: &credentials.SessionCredentials{},
	})
	if err != nil {
		t.Fatal(err)
	}
	cli.inited.Store(true)
	t.Cleanup(func() {
		cli.on.Store(false)
		cli.cancel()
	})
	return cli
}

func newSessionClient(t *testing.T) (*defaultClient, *MockClientManager) {
	t.Helper()
	cli := BuildCLient(t)
	cli.settings = &simpleConsumerSettings{
		clientId:       cli.clientID,
		endpoints:      fakeEndpoints(),
		clientType:     v2.ClientType_SIMPLE_CONSUMER,
		requestTimeout: time.Second,
	}
	manager := NewMockClientManager(gomock.NewController(t))
	cli.clientManager = manager
	return cli, manager
}

func TestSessionSendsSettingsOnStreamCreation(t *testing.T) {
	cli, manager := newSessionClient(t)
	rec := &streamRecorder{}
	manager.EXPECT().Telemetry(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *v2.Endpoints, _ time.Duration) (v2.MessagingService_TelemetryClient, error) {
			s := newFakeTelemetryStream(ctx)
			rec.add(s)
			return s, nil
		}).Times(1)

	session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.release)

	waitForCondition(t, 5*time.Second, func() bool {
		s := rec.at(0)
		return s != nil && s.settingsSends() == 1
	}, "the supervisor to send settings on the new stream")
	assert.NoError(t, session.publish(context.Background(), &v2.TelemetryCommand{}))
	waitForCondition(t, time.Second, func() bool { return rec.at(0).sendCount() == 2 }, "the command to reach the stream")
}

func TestSessionRenewsAfterRecvFailureAndResendsSettings(t *testing.T) {
	cli, manager := newSessionClient(t)
	rec := &streamRecorder{}
	manager.EXPECT().Telemetry(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *v2.Endpoints, _ time.Duration) (v2.MessagingService_TelemetryClient, error) {
			var s *fakeTelemetryStream
			if rec.count() == 0 {
				s = newFakeTelemetryStream(ctx, fakeRecvResult{err: io.EOF})
			} else {
				s = newFakeTelemetryStream(ctx)
			}
			rec.add(s)
			return s, nil
		}).Times(2)

	session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.release)

	waitForCondition(t, 10*time.Second, func() bool {
		first, second := rec.at(0), rec.at(1)
		return second != nil && second.settingsSends() == 1 && first != nil && first.isClosed()
	}, "EOF to renew the stream, resend settings and close the old stream")
}

func TestSessionReconnectCancelsBlockedSettingsSend(t *testing.T) {
	cli, manager := newSessionClient(t)
	rec := &streamRecorder{}
	manager.EXPECT().Telemetry(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *v2.Endpoints, _ time.Duration) (v2.MessagingService_TelemetryClient, error) {
			s := newFakeTelemetryStream(ctx)
			if rec.count() == 0 {
				s.gateSends()
			}
			rec.add(s)
			return s, nil
		}).AnyTimes()

	session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.release)

	waitForCondition(t, 5*time.Second, func() bool {
		first := rec.at(0)
		return first != nil && first.sendBlocked()
	}, "the supervisor settings send to block")
	session.reconnect()
	waitForCondition(t, 10*time.Second, func() bool {
		first, second := rec.at(0), rec.at(1)
		return second != nil && second.settingsSends() == 1 && first != nil && first.isClosed()
	}, "reconnect to cancel the blocked send and renew the stream")
}

func TestSessionPublishBlockedSendIsCanceledByReconnect(t *testing.T) {
	cli, manager := newSessionClient(t)
	rec := &streamRecorder{}
	manager.EXPECT().Telemetry(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *v2.Endpoints, _ time.Duration) (v2.MessagingService_TelemetryClient, error) {
			s := newFakeTelemetryStream(ctx)
			rec.add(s)
			return s, nil
		}).Times(2)

	session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.release)
	waitForCondition(t, 5*time.Second, func() bool {
		first := rec.at(0)
		return first != nil && first.settingsSends() == 1
	}, "the stream to be installed")

	first := rec.at(0)
	publishDone := make(chan error, 1)
	entered := first.gateSends()
	go func() {
		publishDone <- session.publish(context.Background(), &v2.TelemetryCommand{})
	}()
	<-entered
	session.reconnect()
	select {
	case sendErr := <-publishDone:
		assert.Error(t, sendErr, "the canceled stream must fail the blocked writer")
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect did not release the blocked publish")
	}
	waitForCondition(t, 10*time.Second, func() bool {
		second := rec.at(1)
		return second != nil && second.settingsSends() == 1 && first.isClosed()
	}, "the supervisor to renew after the blocked publish")
}

func TestSessionReleaseUnblocksStuckSendAndJoins(t *testing.T) {
	cli, manager := newSessionClient(t)
	rec := &streamRecorder{}
	manager.EXPECT().Telemetry(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *v2.Endpoints, _ time.Duration) (v2.MessagingService_TelemetryClient, error) {
			s := newFakeTelemetryStream(ctx)
			s.gateSends()
			rec.add(s)
			return s, nil
		}).AnyTimes()

	session, err := cli.getDefaultClientSession(fakeAddress)
	if err != nil {
		t.Fatal(err)
	}
	// Pin the supervisor inside the blocked Send before releasing, otherwise
	// release could cancel first and the stream would never be created.
	waitForCondition(t, 5*time.Second, func() bool {
		s := rec.at(0)
		return s != nil && s.sendBlocked()
	}, "the settings send to block on the stuck connection")
	released := make(chan struct{})
	go func() {
		session.release()
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("release hung behind the stuck send")
	}
	assert.True(t, rec.at(0).isClosed(), "the stuck stream must be closed on release")
}

func TestStartupReportsSettingsFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRpc := newStartupMockRpc(ctrl)
	restore := scopeRpcClientFactory(t, mockRpc)
	defer restore()
	mockRpc.EXPECT().QueryRoute(gomock.Any(), gomock.Any()).Return(&v2.QueryRouteResponse{
		Status:        &v2.Status{Code: v2.Code_OK},
		MessageQueues: []*v2.MessageQueue{{Broker: &v2.Broker{Endpoints: fakeEndpoints()}, Permission: v2.Permission_READ_WRITE}},
	}, nil)
	mockRpc.EXPECT().Telemetry(gomock.Any()).Return(nil, errors.New("telemetry unavailable")).AnyTimes()

	cli := newStartupClient(t)
	cli.initTopics = []string{MOCK_TOPIC}
	err := cli.startUp()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get topic route data")
}

func TestStartupCompletesWhenSettingsArrive(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRpc := newStartupMockRpc(ctrl)
	restore := scopeRpcClientFactory(t, mockRpc)
	defer restore()
	mockRpc.EXPECT().QueryRoute(gomock.Any(), gomock.Any()).Return(&v2.QueryRouteResponse{
		Status:        &v2.Status{Code: v2.Code_OK},
		MessageQueues: []*v2.MessageQueue{{Broker: &v2.Broker{Endpoints: fakeEndpoints()}, Permission: v2.Permission_READ_WRITE}},
	}, nil)
	mockRpc.EXPECT().Telemetry(gomock.Any()).DoAndReturn(func(ctx context.Context) (v2.MessagingService_TelemetryClient, error) {
		return newFakeTelemetryStream(ctx, fakeRecvResult{command: subscriptionSettingsCommand()}), nil
	}).AnyTimes()

	cli := newStartupClient(t)
	cli.initTopics = []string{MOCK_TOPIC}
	assert.NoError(t, cli.startUp())
	assert.True(t, cli.inited.Load())
	assert.NoError(t, cli.GracefulStop())
}

func newStartupClient(t *testing.T) *defaultClient {
	t.Helper()
	cli, err := NewClientConcrete(&Config{
		Endpoint:    fmt.Sprintf("%s:%d", fakeHost, fakePort),
		Credentials: &credentials.SessionCredentials{},
	})
	if err != nil {
		t.Fatal(err)
	}
	cli.settings = &simpleConsumerSettings{
		clientId:       cli.clientID,
		endpoints:      fakeEndpoints(),
		clientType:     v2.ClientType_SIMPLE_CONSUMER,
		requestTimeout: time.Second,
	}
	t.Cleanup(func() {
		cli.on.Store(false)
		cli.cancel()
		if manager, ok := cli.clientManager.(*defaultClientManager); ok {
			manager.shutdown()
		}
	})
	return cli
}

func newStartupMockRpc(ctrl *gomock.Controller) *MockRpcClient {
	mockRpc := NewMockRpcClient(ctrl)
	mockRpc.EXPECT().HeartBeat(gomock.Any(), gomock.Any()).
		Return(&v2.HeartbeatResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil).AnyTimes()
	mockRpc.EXPECT().NotifyClientTermination(gomock.Any(), gomock.Any()).
		Return(&v2.NotifyClientTerminationResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil).AnyTimes()
	mockRpc.EXPECT().GracefulStop().Return(nil).AnyTimes()
	mockRpc.EXPECT().idleDuration().Return(time.Duration(0)).AnyTimes()
	mockRpc.EXPECT().GetTarget().Return(fakeAddress).AnyTimes()
	return mockRpc
}

// scopeRpcClientFactory swaps the package factory only for this serial test and
// restores it before the controller finishes, so the real dialer is never
// reachable from later tests.
func scopeRpcClientFactory(t *testing.T, rpc RpcClient) func() {
	t.Helper()
	real := NewRpcClient
	NewRpcClient = func(target string, opts ...RpcClientOption) (RpcClient, error) {
		return rpc, nil
	}
	var once sync.Once
	restore := func() { once.Do(func() { NewRpcClient = real }) }
	t.Cleanup(restore)
	return restore
}

func TestUpdateRouteRetiresRemovedEndpoints(t *testing.T) {
	cli, manager := newSessionClient(t)
	rec := &streamRecorder{}
	manager.EXPECT().Telemetry(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *v2.Endpoints, _ time.Duration) (v2.MessagingService_TelemetryClient, error) {
			s := newFakeTelemetryStream(ctx)
			rec.add(s)
			return s, nil
		}).Times(1)

	route := []*v2.MessageQueue{{Broker: &v2.Broker{Endpoints: fakeEndpoints()}, Permission: v2.Permission_READ_WRITE}}
	assert.NoError(t, cli.updateRoute(MOCK_TOPIC, route))
	assert.False(t, cli.isEndpointsDeprecated(fakeEndpoints()))

	assert.NoError(t, cli.updateRoute(MOCK_TOPIC, nil))
	waitForCondition(t, 5*time.Second, func() bool {
		cli.endpointsTelemetryClientsLock.RLock()
		defer cli.endpointsTelemetryClientsLock.RUnlock()
		s := rec.at(0)
		return len(cli.endpointsTelemetryClientTable) == 0 && s != nil && s.isClosed()
	}, "the retired session to be removed and its stream closed")
	assert.True(t, cli.isEndpointsDeprecated(fakeEndpoints()))
}

func TestPendingInitializationKeepsEndpointAlive(t *testing.T) {
	cli, manager := newSessionClient(t)
	releaseTelemetry := make(chan struct{})
	telemetryCalled := make(chan struct{})
	var once sync.Once
	rec := &streamRecorder{}
	manager.EXPECT().Telemetry(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *v2.Endpoints, _ time.Duration) (v2.MessagingService_TelemetryClient, error) {
			once.Do(func() { close(telemetryCalled) })
			<-releaseTelemetry
			s := newFakeTelemetryStream(ctx)
			rec.add(s)
			return s, nil
		}).AnyTimes()

	route := []*v2.MessageQueue{{Broker: &v2.Broker{Endpoints: fakeEndpoints()}, Permission: v2.Permission_READ_WRITE}}
	updated := make(chan error, 1)
	go func() { updated <- cli.updateRoute(MOCK_TOPIC, route) }()
	<-telemetryCalled
	// The route is not published yet; only the pending reference keeps the
	// initializing endpoint from being treated as deprecated.
	assert.False(t, cli.isEndpointsDeprecated(fakeEndpoints()))
	assert.Empty(t, cli.getTotalTargets())
	close(releaseTelemetry)
	assert.NoError(t, <-updated)
	assert.False(t, cli.isEndpointsDeprecated(fakeEndpoints()))
	waitForCondition(t, 5*time.Second, func() bool {
		s := rec.at(0)
		return s != nil && s.settingsSends() == 1
	}, "settings on the published stream")
}

func TestStoppedClientRejectsRouteAndSessionWork(t *testing.T) {
	cli, _ := newSessionClient(t)
	cli.on.Store(false)
	cli.cancel()

	_, err := cli.getDefaultClientSession(fakeAddress)
	assert.ErrorIs(t, err, context.Canceled)
	err = cli.updateRoute(MOCK_TOPIC, []*v2.MessageQueue{{Broker: &v2.Broker{Endpoints: fakeEndpoints()}}})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, cli.getSessionIfPresent(fakeEndpoints()))
	assert.True(t, cli.isEndpointsDeprecated(fakeEndpoints()))
}

func TestReconnectEndpointsCommandDrivesTransportRecovery(t *testing.T) {
	cm, cli := newRecoveryTestManager(t)
	started := make(chan struct{})
	blocked, release := recoveryBarrier(t)
	next := &recoveryTestRPC{}
	old := &recoveryTestRPC{state: connectivity.Ready, rebuild: func(context.Context) (RpcClient, error) {
		close(started)
		<-blocked
		return next, nil
	}}
	installRecoveryRPC(cm, old)

	cli.onReconnectEndpointsCommand(fakeEndpoints(), &v2.ReconnectEndpointsCommand{})
	assert.True(t, cli.getReceiveReconnect())
	waitRecoverySignal(t, started)
	release()
	waitForCondition(t, 5*time.Second, func() bool {
		entry := recoveryEntry(cm)
		return entry != nil && entry.rpc == next && old.closed.Load() == 1
	}, "the server command to replace the transport")
}

func TestHandleTelemetryCommand(t *testing.T) {
	cli := BuildCLient(t)
	cli.settings = &simpleConsumerSettings{
		clientId:       cli.clientID,
		endpoints:      fakeEndpoints(),
		clientType:     v2.ClientType_SIMPLE_CONSUMER,
		requestTimeout: time.Second,
	}
	session := &defaultClientSession{endpoints: fakeEndpoints(), cli: cli}

	assert.Error(t, session.handleTelemetryCommand(&v2.TelemetryCommand{}))
	assert.NoError(t, session.handleTelemetryCommand(&v2.TelemetryCommand{
		Command: &v2.TelemetryCommand_RecoverOrphanedTransactionCommand{RecoverOrphanedTransactionCommand: &v2.RecoverOrphanedTransactionCommand{}},
	}))
	assert.NoError(t, session.handleTelemetryCommand(subscriptionSettingsCommand()))
	assert.True(t, cli.inited.Load())
}

func TestReceiveReconnectFlag(t *testing.T) {
	cli := BuildCLient(t)
	assert.False(t, cli.getReceiveReconnect())
	cli.setReceiveReconnect(true)
	assert.True(t, cli.getReceiveReconnect())
	producer := &defaultProducer{cli: cli}
	cli.clientImpl = producer
	assert.True(t, cli.clientImpl.IsEndpointUpdated())
	producer.SetReceiveReconnect(false)
	assert.False(t, cli.getReceiveReconnect())
}

func TestCLINewClient(t *testing.T) {
	cli, err := NewClient(&Config{
		Endpoint:    fmt.Sprintf("%s:%d", fakeHost, fakePort),
		Credentials: &credentials.SessionCredentials{},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, cli.GetClientID())
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
