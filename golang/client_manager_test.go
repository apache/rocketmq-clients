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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/golang/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

var MOCK_CLIENT_ID = "mock_client_id"
var MOCK_TOPIC = "mock_topic"
var MOCK_GROUP = "mock_group"

var (
	fakeHost          = "127.0.0.1"
	fakePort    int32 = 80
	fakeScheme        = "ip"
	fakeAddress       = fmt.Sprintf("%s:///%s:%d", fakeScheme, fakeHost, fakePort)
)

func fakeEndpoints() *v2.Endpoints {
	return &v2.Endpoints{Scheme: v2.AddressScheme_IPv4, Addresses: []*v2.Address{{Host: fakeHost, Port: fakePort}}}
}

func TestMain(m *testing.M) {
	ResetLogger()
	os.Exit(m.Run())
}

type recoveryTestRPC struct {
	RpcClient
	state     connectivity.State
	idle      time.Duration
	closed    atomic.Int32
	rebuild   func(context.Context) (RpcClient, error)
	heartbeat func(context.Context) (*v2.HeartbeatResponse, error)
	closeFn   func()
}

func (rpc *recoveryTestRPC) GetTarget() string            { return fakeAddress }
func (rpc *recoveryTestRPC) getState() connectivity.State { return rpc.state }
func (rpc *recoveryTestRPC) idleDuration() time.Duration  { return rpc.idle }
func (rpc *recoveryTestRPC) GracefulStop() error {
	rpc.closed.Add(1)
	if rpc.closeFn != nil {
		rpc.closeFn()
	}
	return nil
}
func (rpc *recoveryTestRPC) recreate(ctx context.Context) (RpcClient, error) {
	if rpc.rebuild == nil {
		return nil, errors.New("unexpected recovery")
	}
	return rpc.rebuild(ctx)
}
func (rpc *recoveryTestRPC) HeartBeat(ctx context.Context, _ *v2.HeartbeatRequest) (*v2.HeartbeatResponse, error) {
	if rpc.heartbeat != nil {
		return rpc.heartbeat(ctx)
	}
	return &v2.HeartbeatResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
}

func newRecoveryTestManager(t *testing.T) (*defaultClientManager, *defaultClient) {
	t.Helper()
	cm := NewDefaultClientManager()
	t.Cleanup(cm.shutdown)
	cli := &defaultClient{
		clientID:                      MOCK_CLIENT_ID,
		endpointsTelemetryClientTable: make(map[string]*defaultClientSession),
		pendingTargets:                make(map[string]int),
	}
	cli.on.Store(true)
	cli.clientManager = cm
	cli.router.Store(MOCK_TOPIC, []*v2.MessageQueue{{Broker: &v2.Broker{Endpoints: fakeEndpoints()}}})
	cm.RegisterClient(cli)
	return cm, cli
}

func installRecoveryRPC(cm *defaultClientManager, rpc RpcClient) *rpcClientEntry {
	entry := &rpcClientEntry{target: fakeAddress, endpoints: fakeEndpoints(), rpc: rpc}
	cm.rpcClientTableLock.Lock()
	cm.rpcClientTable[fakeAddress] = entry
	cm.rpcClientTableLock.Unlock()
	return entry
}

func recoveryEntry(cm *defaultClientManager) *rpcClientEntry {
	cm.rpcClientTableLock.RLock()
	defer cm.rpcClientTableLock.RUnlock()
	return cm.rpcClientTable[fakeAddress]
}

func recoveryJob(cm *defaultClientManager) *rpcClientJob {
	cm.rpcClientTableLock.RLock()
	defer cm.rpcClientTableLock.RUnlock()
	return cm.rpcClientJobs[fakeAddress]
}

func waitRecoverySignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal("timed out waiting for recovery barrier")
	}
}

// Barriers are released by the test (including failure cleanup), never by an
// automatic timer that could allow an overlapping recovery to go unnoticed.
func recoveryBarrier(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	channel := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(channel) }) }
	t.Cleanup(release)
	return channel, release
}

func TestCMRegisterArbitraryClient(t *testing.T) {
	cm := NewDefaultClientManager()
	defer cm.shutdown()
	client := NewMockClient(gomock.NewController(t))
	client.EXPECT().GetClientID().Return(MOCK_CLIENT_ID).AnyTimes()
	cm.RegisterClient(client)
	if actual, ok := cm.clientTable.Load(MOCK_CLIENT_ID); !ok || actual != client {
		t.Fatal("registered client missing")
	}
	cm.doHeartbeat()
	cm.syncSettings()
	cm.UnRegisterClient(client)
	if _, ok := cm.clientTable.Load(MOCK_CLIENT_ID); ok {
		t.Fatal("unregistered client retained")
	}
}

func TestCMHeartbeatThresholdAndStaleCompletion(t *testing.T) {
	cm, _ := newRecoveryTestManager(t)
	started := make(chan struct{})
	blocked, release := recoveryBarrier(t)
	next := &recoveryTestRPC{state: connectivity.Ready}
	old := &recoveryTestRPC{state: connectivity.Ready, heartbeat: func(context.Context) (*v2.HeartbeatResponse, error) {
		return nil, status.Error(codes.DeadlineExceeded, "half open")
	}, rebuild: func(context.Context) (RpcClient, error) {
		close(started)
		<-blocked
		return next, nil
	}}
	entry := installRecoveryRPC(cm, old)
	for i := 0; i < heartbeatRecoveryThreshold; i++ {
		_, err := cm.HeartBeat(context.Background(), fakeEndpoints(), &v2.HeartbeatRequest{}, time.Second)
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatal(err)
		}
		if i == 0 && recoveryJob(cm) != nil {
			t.Fatal("first timeout started recovery")
		}
	}
	waitRecoverySignal(t, started)
	job := recoveryJob(cm)
	if job == nil || recoveryEntry(cm) != entry {
		t.Fatal("old transport must remain published during the dial")
	}
	cached, err := cm.getRpcClientContext(context.Background(), fakeEndpoints())
	if err != nil || cached.rpc != old {
		t.Fatal("cache hit did not return interface implementation", err)
	}
	release()
	waitRecoverySignal(t, job.done)
	current := recoveryEntry(cm)
	if current == entry || current.rpc != next || old.closed.Load() != 1 || current.deadlineFailures != 0 {
		t.Fatal("replacement did not install a fresh generation and close the old transport")
	}
	cm.recordHeartbeat(entry, status.Error(codes.DeadlineExceeded, "late heartbeat"))
	if recoveryEntry(cm) != current || current.deadlineFailures != 0 || next.closed.Load() != 0 {
		t.Fatal("old RPC completion changed its successor")
	}
}

func TestCMHeartbeatClearsFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		state connectivity.State
	}{
		{name: "success"},
		{name: "other error", err: status.Error(codes.Internal, "error")},
		{name: "canceled", err: context.Canceled},
		{name: "unavailable idle", err: status.Error(codes.Unavailable, "error"), state: connectivity.Idle},
		{name: "unavailable connecting", err: status.Error(codes.Unavailable, "error"), state: connectivity.Connecting},
		{name: "unavailable transient failure", err: status.Error(codes.Unavailable, "error"), state: connectivity.TransientFailure},
		{name: "unavailable shutdown", err: status.Error(codes.Unavailable, "error"), state: connectivity.Shutdown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm, _ := newRecoveryTestManager(t)
			entry := installRecoveryRPC(cm, &recoveryTestRPC{state: tc.state})
			cm.recordHeartbeat(entry, context.DeadlineExceeded)
			cm.recordHeartbeat(entry, tc.err)
			if entry.deadlineFailures != 0 || recoveryJob(cm) != nil || !entry.lastRecovery.IsZero() {
				t.Fatal("success/non-Ready failure did not clear timeout sequence")
			}
			cm.recordHeartbeat(entry, context.DeadlineExceeded)
			if entry.deadlineFailures != 1 || recoveryJob(cm) != nil {
				t.Fatal("timeout sequence was not restarted")
			}
		})
	}
}

func TestCMReadyUnavailableAndCooldown(t *testing.T) {
	cm, _ := newRecoveryTestManager(t)
	started := make(chan struct{}, 2)
	blocked, release := recoveryBarrier(t)
	var attempts atomic.Int32
	old := &recoveryTestRPC{state: connectivity.Ready, rebuild: func(context.Context) (RpcClient, error) {
		attempts.Add(1)
		started <- struct{}{}
		<-blocked
		return nil, errors.New("dial failed")
	}}
	entry := installRecoveryRPC(cm, old)
	cm.recordHeartbeat(entry, status.Error(codes.Unavailable, "Ready but unavailable"))
	waitRecoverySignal(t, started)
	job := recoveryJob(cm)
	release()
	waitRecoverySignal(t, job.done)
	if old.closed.Load() != 0 || recoveryEntry(cm) != entry || entry.deadlineFailures != 0 {
		t.Fatal("failed recreation must retain the old RPC and clear unavailable count")
	}
	cm.recordHeartbeat(entry, context.DeadlineExceeded)
	cm.recordHeartbeat(entry, context.DeadlineExceeded)
	if attempts.Load() != 1 || recoveryJob(cm) != nil || entry.deadlineFailures != 2 {
		t.Fatal("cooldown must suppress recovery without dropping timeout counts")
	}
	cm.rpcClientTableLock.Lock()
	entry.lastRecovery = time.Now().Add(-heartbeatRecoveryCooldown - time.Second)
	cm.rpcClientTableLock.Unlock()
	cm.recordHeartbeat(entry, context.DeadlineExceeded)
	waitRecoverySignal(t, started)
	// The factory may already have finished, so synchronize through managed work.
	cm.workers.Wait()
	if attempts.Load() != 2 || old.closed.Load() != 0 {
		t.Fatal("expired cooldown did not permit the next recovery attempt")
	}
}

func TestCMServerBypassesCooldownButNotInProgress(t *testing.T) {
	cm, _ := newRecoveryTestManager(t)
	started := make(chan struct{})
	blocked, release := recoveryBarrier(t)
	var calls atomic.Int32
	next := &recoveryTestRPC{}
	old := &recoveryTestRPC{state: connectivity.Ready, rebuild: func(context.Context) (RpcClient, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-blocked
		return next, nil
	}}
	entry := installRecoveryRPC(cm, old)
	entry.lastRecovery = time.Now()
	cm.recordHeartbeat(entry, context.DeadlineExceeded)
	cm.recordHeartbeat(entry, context.DeadlineExceeded)
	if recoveryJob(cm) != nil {
		t.Fatal("heartbeat bypassed cooldown")
	}
	cm.reconnect(fakeEndpoints())
	waitRecoverySignal(t, started)
	job := recoveryJob(cm)
	var requests sync.WaitGroup
	for i := 0; i < 32; i++ {
		requests.Add(1)
		go func() {
			defer requests.Done()
			cm.reconnect(fakeEndpoints())
			cm.recordHeartbeat(entry, context.DeadlineExceeded)
		}()
	}
	requests.Wait()
	if calls.Load() != 1 || recoveryJob(cm) != job {
		t.Fatal("server or heartbeat bypassed in-progress guard")
	}
	release()
	waitRecoverySignal(t, job.done)
	if old.closed.Load() != 1 || recoveryEntry(cm).rpc != next {
		t.Fatal("server-directed replacement failed")
	}
}

func TestCMPruneKeepsTransportAndClearsRecoveryState(t *testing.T) {
	cm, cli := newRecoveryTestManager(t)
	cli.accessPoint = fakeEndpoints()
	rpc := &recoveryTestRPC{}
	entry := installRecoveryRPC(cm, rpc)
	entry.deadlineFailures = 1
	entry.lastRecovery = time.Now()
	cli.router.Delete(MOCK_TOPIC)
	cm.pruneEndpoints()
	// The cached transport belongs to the pre-existing idle reaper; prune only
	// drops the recovery bookkeeping of the deprecated endpoint.
	if recoveryEntry(cm) != entry || entry.rpc != rpc || rpc.closed.Load() != 0 {
		t.Fatal("prune must not retire the cached transport")
	}
	if entry.deadlineFailures != 0 || !entry.lastRecovery.IsZero() {
		t.Fatal("prune did not clear the recovery bookkeeping")
	}
	cm.recordHeartbeat(entry, context.DeadlineExceeded)
	if entry.deadlineFailures != 1 || recoveryJob(cm) != nil {
		t.Fatal("bookkeeping did not restart from zero")
	}
}

func TestCMRetirementCancelsInflightRecoveryButKeepsTransport(t *testing.T) {
	cm, cli := newRecoveryTestManager(t)
	started := make(chan struct{}, 2)
	blocked, release := recoveryBarrier(t)
	candidate := &recoveryTestRPC{}
	next := &recoveryTestRPC{}
	var rebuilt atomic.Int32
	old := &recoveryTestRPC{rebuild: func(context.Context) (RpcClient, error) {
		started <- struct{}{}
		<-blocked // deliberately return a late candidate despite cancellation
		if rebuilt.Add(1) == 1 {
			return candidate, nil
		}
		return next, nil
	}}
	entry := installRecoveryRPC(cm, old)
	cm.reconnect(fakeEndpoints())
	waitRecoverySignal(t, started)
	job := recoveryJob(cm)
	cli.router.Delete(MOCK_TOPIC)
	cm.pruneEndpoints()
	if recoveryEntry(cm) != entry || old.closed.Load() != 0 || job.ctx.Err() == nil || recoveryJob(cm) != job {
		t.Fatal("retirement must cancel the in-flight recovery while keeping the transport and its guard")
	}
	cli.router.Store(MOCK_TOPIC, []*v2.MessageQueue{{Broker: &v2.Broker{Endpoints: fakeEndpoints()}}})
	cm.reconnect(fakeEndpoints())
	if recoveryJob(cm) != job || rebuilt.Load() != 0 {
		t.Fatal("rejoining released the outstanding recovery guard")
	}
	release()
	waitRecoverySignal(t, job.done)
	if candidate.closed.Load() != 1 || recoveryEntry(cm).rpc != old {
		t.Fatal("the canceled recovery published or leaked its candidate")
	}
	// The rejoined endpoint recovers through a fresh job.
	cm.reconnect(fakeEndpoints())
	waitRecoverySignal(t, started)
	cm.workers.Wait()
	if recoveryEntry(cm).rpc != next || old.closed.Load() != 1 {
		t.Fatal("the rejoined endpoint did not recover through a fresh job")
	}
}

func TestCMInitialAccessPointDial(t *testing.T) {
	cm, cli := newRecoveryTestManager(t)
	cli.router.Delete(MOCK_TOPIC)
	cli.accessPoint = fakeEndpoints()
	mock := NewMockRpcClient(gomock.NewController(t))
	mock.EXPECT().QueryRoute(gomock.Any(), gomock.Any()).Return(&v2.QueryRouteResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil)
	rpc := &recoveryTestRPC{RpcClient: mock}
	cm.rpcClientFactory = func(string, ...RpcClientOption) (RpcClient, error) { return rpc, nil }
	response, err := cm.QueryRoute(context.Background(), fakeEndpoints(), &v2.QueryRouteRequest{}, time.Second)
	if err != nil || response.GetStatus().GetCode() != v2.Code_OK {
		t.Fatal("access-point QueryRoute was forbidden before route publication", err)
	}
}

func TestCMShutdownCancelsAndWaitsForLateCandidate(t *testing.T) {
	cm, _ := newRecoveryTestManager(t)
	started := make(chan struct{})
	canceled := make(chan struct{})
	blocked, release := recoveryBarrier(t)
	candidate := &recoveryTestRPC{}
	old := &recoveryTestRPC{rebuild: func(ctx context.Context) (RpcClient, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-blocked
		return candidate, nil
	}}
	installRecoveryRPC(cm, old)
	cm.reconnect(fakeEndpoints())
	waitRecoverySignal(t, started)
	stopped := make(chan struct{})
	go func() { cm.shutdown(); close(stopped) }()
	waitRecoverySignal(t, canceled)
	select {
	case <-stopped:
		t.Fatal("shutdown returned while its candidate was still running")
	default:
	}
	release()
	waitRecoverySignal(t, stopped)
	cm.shutdown()
	if candidate.closed.Load() != 1 || old.closed.Load() != 1 || recoveryEntry(cm) != nil {
		t.Fatal("shutdown leaked/published a late candidate")
	}
}

func TestCMInitialDialShutdownAndUnlockedCache(t *testing.T) {
	cm := NewDefaultClientManager()
	t.Cleanup(cm.shutdown)
	started := make(chan struct{})
	canceled := make(chan struct{})
	blocked, release := recoveryBarrier(t)
	candidate := &recoveryTestRPC{}
	other := &recoveryTestRPC{}
	cm.rpcClientFactory = func(target string, opts ...RpcClientOption) (RpcClient, error) {
		if target != fakeAddress {
			return other, nil
		}
		options := rpcClientOptions{}
		for _, opt := range opts {
			opt.apply(&options)
		}
		connOptions := connOptions{}
		for _, opt := range options.connOptions {
			opt.apply(&connOptions)
		}
		close(started)
		<-connOptions.Context.Done()
		close(canceled)
		<-blocked
		return candidate, nil
	}
	callerDone := make(chan struct{})
	go func() {
		defer close(callerDone)
		_, err := cm.getRpcClientContext(context.Background(), fakeEndpoints())
		if !errors.Is(err, context.Canceled) {
			t.Errorf("initial dial returned %v", err)
		}
	}()
	waitRecoverySignal(t, started)
	otherEndpoints := fakeEndpoints()
	otherEndpoints.Addresses[0].Port++
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if entry, err := cm.getRpcClientContext(ctx, otherEndpoints); err != nil || entry.rpc != other {
		t.Fatal("unrelated dial blocked behind cache lock", err)
	}
	stopped := make(chan struct{})
	go func() { cm.shutdown(); close(stopped) }()
	waitRecoverySignal(t, canceled)
	waitRecoverySignal(t, callerDone)
	release()
	waitRecoverySignal(t, stopped)
	if candidate.closed.Load() != 1 || other.closed.Load() != 1 {
		t.Fatal("initial dial shutdown leaked connections")
	}
}

func TestCMScheduledInitialDelayIsCancellable(t *testing.T) {
	cm := NewDefaultClientManager()
	cm.opts.HEART_BEAT_INITIAL_DELAY = time.Hour
	cm.opts.RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY = time.Hour
	cm.opts.LOG_STATS_INITIAL_DELAY = time.Hour
	cm.opts.SYNC_SETTINGS_DELAY = time.Hour
	cm.startUp()
	stopped := make(chan struct{})
	go func() { cm.shutdown(); close(stopped) }()
	waitRecoverySignal(t, stopped)
	cm.startUp()
	cm.shutdown()
}

func TestCMIdleReapClosesOutsideCacheLock(t *testing.T) {
	cm, _ := newRecoveryTestManager(t)
	closed := make(chan struct{})
	rpc := &recoveryTestRPC{idle: time.Hour, closeFn: func() {
		cm.rpcClientTableLock.Lock()
		cm.rpcClientTableLock.Unlock()
		close(closed)
	}}
	entry := installRecoveryRPC(cm, rpc)
	entry.deadlineFailures = 1
	finished := make(chan struct{})
	go func() { cm.clearIdleRpcClients(); close(finished) }()
	waitRecoverySignal(t, closed)
	waitRecoverySignal(t, finished)
	if recoveryEntry(cm) != nil || rpc.closed.Load() != 1 {
		t.Fatal("idle transport/state not retired")
	}
}

func TestCMUnaryBoundaries(t *testing.T) {
	for _, operation := range []struct {
		name   string
		expect func(*MockRpcClient) *gomock.Call
		invoke func(*defaultClientManager) error
	}{
		{"query route", func(r *MockRpcClient) *gomock.Call { return r.EXPECT().QueryRoute(gomock.Any(), gomock.Any()) }, func(cm *defaultClientManager) error {
			_, err := cm.QueryRoute(context.Background(), fakeEndpoints(), &v2.QueryRouteRequest{}, time.Hour)
			return err
		}},
		{"assignments", func(r *MockRpcClient) *gomock.Call { return r.EXPECT().QueryAssignments(gomock.Any(), gomock.Any()) }, func(cm *defaultClientManager) error {
			_, err := cm.QueryAssignments(context.Background(), fakeEndpoints(), &v2.QueryAssignmentRequest{}, time.Hour)
			return err
		}},
		{"send", func(r *MockRpcClient) *gomock.Call { return r.EXPECT().SendMessage(gomock.Any(), gomock.Any()) }, func(cm *defaultClientManager) error {
			_, err := cm.SendMessage(context.Background(), fakeEndpoints(), &v2.SendMessageRequest{}, time.Hour)
			return err
		}},
		{"transaction", func(r *MockRpcClient) *gomock.Call { return r.EXPECT().EndTransaction(gomock.Any(), gomock.Any()) }, func(cm *defaultClientManager) error {
			_, err := cm.EndTransaction(context.Background(), fakeEndpoints(), &v2.EndTransactionRequest{}, time.Hour)
			return err
		}},
		{"termination", func(r *MockRpcClient) *gomock.Call {
			return r.EXPECT().NotifyClientTermination(gomock.Any(), gomock.Any())
		}, func(cm *defaultClientManager) error {
			_, err := cm.NotifyClientTermination(context.Background(), fakeEndpoints(), &v2.NotifyClientTerminationRequest{}, time.Hour)
			return err
		}},
		{"ack", func(r *MockRpcClient) *gomock.Call { return r.EXPECT().AckMessage(gomock.Any(), gomock.Any()) }, func(cm *defaultClientManager) error {
			_, err := cm.AckMessage(context.Background(), fakeEndpoints(), &v2.AckMessageRequest{}, time.Hour)
			return err
		}},
		{"invisibility", func(r *MockRpcClient) *gomock.Call {
			return r.EXPECT().ChangeInvisibleDuration(gomock.Any(), gomock.Any())
		}, func(cm *defaultClientManager) error {
			_, err := cm.ChangeInvisibleDuration(context.Background(), fakeEndpoints(), &v2.ChangeInvisibleDurationRequest{}, time.Hour)
			return err
		}},
		{"dead letter", func(r *MockRpcClient) *gomock.Call {
			return r.EXPECT().ForwardMessageToDeadLetterQueue(gomock.Any(), gomock.Any())
		}, func(cm *defaultClientManager) error {
			_, err := cm.ForwardMessageToDeadLetterQueue(context.Background(), fakeEndpoints(), &v2.ForwardMessageToDeadLetterQueueRequest{}, time.Hour)
			return err
		}},
		{"lite subscription", func(r *MockRpcClient) *gomock.Call {
			return r.EXPECT().SyncLiteSubscription(gomock.Any(), gomock.Any())
		}, func(cm *defaultClientManager) error {
			_, err := cm.SyncLiteSubscription(context.Background(), fakeEndpoints(), &v2.SyncLiteSubscriptionRequest{}, time.Hour)
			return err
		}},
		{"recall", func(r *MockRpcClient) *gomock.Call { return r.EXPECT().RecallMessage(gomock.Any(), gomock.Any()) }, func(cm *defaultClientManager) error {
			_, err := cm.RecallMessage(context.Background(), fakeEndpoints(), &v2.RecallMessageRequest{}, time.Hour)
			return err
		}},
	} {
		// Two representatives per wrapper: full normalizeGrpcError code coverage
		// lives in error_test.go, so here we only prove each hand-written unary
		// wrapper cancels its timeout, normalizes, and never triggers recovery.
		for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted} {
			t.Run(operation.name+"/"+code.String(), func(t *testing.T) {
				cm, _ := newRecoveryTestManager(t)
				mock := NewMockRpcClient(gomock.NewController(t))
				rpc := &recoveryTestRPC{RpcClient: mock, state: connectivity.Ready}
				entry := installRecoveryRPC(cm, rpc)
				original := status.Error(code, "transport error")
				var observed context.Context
				operation.expect(mock).Do(func(ctx context.Context, _ interface{}) { observed = ctx }).Return(nil, original)
				err := operation.invoke(cm)
				assertManagerNormalizedError(t, err, original)
				if observed == nil || observed.Err() == nil {
					t.Fatal("unary timeout context was not canceled")
				}
				if recoveryEntry(cm) != entry || recoveryJob(cm) != nil || entry.deadlineFailures != 0 || rpc.closed.Load() != 0 {
					t.Fatal("ordinary unary RPC triggered recovery")
				}
			})
		}
	}
}

func assertManagerNormalizedError(t *testing.T, actual, original error) {
	t.Helper()
	if status.Code(original) == codes.ResourceExhausted {
		rpcStatus, ok := AsErrRpcStatus(actual)
		if !ok || rpcStatus.Code != int32(v2.Code_TOO_MANY_REQUESTS) || !errors.Is(actual, original) {
			t.Fatalf("expected normalized error preserving cause, got %v", actual)
		}
	} else if actual != original {
		t.Fatalf("non-throttle error changed: %v -> %v", original, actual)
	}
}

type managerReceiveStream struct {
	grpc.ClientStream
	err error
}

func (stream *managerReceiveStream) Recv() (*v2.ReceiveMessageResponse, error) {
	return nil, stream.err
}
func (stream *managerReceiveStream) RecvMsg(interface{}) error { return stream.err }

func TestCMReceiveMessageNormalization(t *testing.T) {
	for _, original := range []error{nil, io.EOF, context.Canceled, status.Error(codes.ResourceExhausted, "limited")} {
		cm := NewDefaultClientManager()
		mock := NewMockRpcClient(gomock.NewController(t))
		installRecoveryRPC(cm, &recoveryTestRPC{RpcClient: mock})
		mock.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any()).Return(&managerReceiveStream{err: original}, nil)
		stream, err := cm.ReceiveMessage(context.Background(), fakeEndpoints(), &v2.ReceiveMessageRequest{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = stream.Recv()
		assertManagerNormalizedError(t, err, original)
		assertManagerNormalizedError(t, stream.RecvMsg(&v2.ReceiveMessageResponse{}), original)
		mock.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any()).Return(nil, original)
		_, err = cm.ReceiveMessage(context.Background(), fakeEndpoints(), &v2.ReceiveMessageRequest{})
		assertManagerNormalizedError(t, err, original)
		cm.shutdown()
	}
}

type managerTelemetryStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (stream *managerTelemetryStream) Context() context.Context            { return stream.ctx }
func (stream *managerTelemetryStream) Send(*v2.TelemetryCommand) error     { return nil }
func (stream *managerTelemetryStream) Recv() (*v2.TelemetryCommand, error) { return nil, nil }
func (stream *managerTelemetryStream) CloseSend() error                    { return nil }

func TestCMTelemetryStreamLifetimeBelongsToCaller(t *testing.T) {
	cm := NewDefaultClientManager()
	defer cm.shutdown()
	mock := NewMockRpcClient(gomock.NewController(t))
	installRecoveryRPC(cm, &recoveryTestRPC{RpcClient: mock})
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw := &managerTelemetryStream{}
	mock.EXPECT().Telemetry(gomock.Any()).DoAndReturn(func(ctx context.Context) (v2.MessagingService_TelemetryClient, error) {
		raw.ctx = ctx
		return raw, nil
	})
	stream, err := cm.Telemetry(parent, fakeEndpoints(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if stream != v2.MessagingService_TelemetryClient(raw) || raw.ctx != parent {
		t.Fatal("the stream and its context must pass through unchanged")
	}
	if err := stream.Send(&v2.TelemetryCommand{}); err != nil {
		t.Fatal("the stream must stay usable after the call returned", err)
	}
	cancel()
	if raw.ctx.Err() == nil {
		t.Fatal("the caller context owns the stream lifetime")
	}
}

func TestCMTelemetryCreationFailureIsNormalized(t *testing.T) {
	cm := NewDefaultClientManager()
	defer cm.shutdown()
	mock := NewMockRpcClient(gomock.NewController(t))
	installRecoveryRPC(cm, &recoveryTestRPC{RpcClient: mock})
	original := status.Error(codes.ResourceExhausted, "limited")
	mock.EXPECT().Telemetry(gomock.Any()).Return(nil, original)
	_, err := cm.Telemetry(context.Background(), fakeEndpoints(), time.Hour)
	assertManagerNormalizedError(t, err, original)
}
