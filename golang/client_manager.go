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
	"sync"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ClientManager interface {
	RegisterClient(client Client)
	UnRegisterClient(client Client)
	QueryRoute(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryRouteRequest, duration time.Duration) (*v2.QueryRouteResponse, error)
	QueryAssignments(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryAssignmentRequest, duration time.Duration) (*v2.QueryAssignmentResponse, error)
	HeartBeat(ctx context.Context, endpoints *v2.Endpoints, request *v2.HeartbeatRequest, duration time.Duration) (*v2.HeartbeatResponse, error)
	SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error)
	Telemetry(ctx context.Context, endpoints *v2.Endpoints, duration time.Duration) (v2.MessagingService_TelemetryClient, error)
	EndTransaction(ctx context.Context, endpoints *v2.Endpoints, request *v2.EndTransactionRequest, duration time.Duration) (*v2.EndTransactionResponse, error)
	NotifyClientTermination(ctx context.Context, endpoints *v2.Endpoints, request *v2.NotifyClientTerminationRequest, duration time.Duration) (*v2.NotifyClientTerminationResponse, error)
	ReceiveMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error)
	AckMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.AckMessageRequest, duration time.Duration) (*v2.AckMessageResponse, error)
	ChangeInvisibleDuration(ctx context.Context, endpoints *v2.Endpoints, request *v2.ChangeInvisibleDurationRequest, duration time.Duration) (*v2.ChangeInvisibleDurationResponse, error)
	ForwardMessageToDeadLetterQueue(ctx context.Context, endpoints *v2.Endpoints, request *v2.ForwardMessageToDeadLetterQueueRequest, duration time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error)
	SyncLiteSubscription(ctx context.Context, endpoints *v2.Endpoints, request *v2.SyncLiteSubscriptionRequest, duration time.Duration) (*v2.SyncLiteSubscriptionResponse, error)
	RecallMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.RecallMessageRequest, duration time.Duration) (*v2.RecallMessageResponse, error)
	shutdown()
}

type clientManagerOptions struct {
	RPC_CLIENT_MAX_IDLE_DURATION time.Duration

	RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY time.Duration
	RPC_CLIENT_IDLE_CHECK_PERIOD        time.Duration

	HEART_BEAT_INITIAL_DELAY time.Duration
	HEART_BEAT_PERIOD        time.Duration

	LOG_STATS_INITIAL_DELAY time.Duration
	LOG_STATS_PERIOD        time.Duration

	SYNC_SETTINGS_DELAY  time.Duration
	SYNC_SETTINGS_PERIOD time.Duration
}

var defaultClientManagerOptions = clientManagerOptions{
	RPC_CLIENT_MAX_IDLE_DURATION: time.Minute * 30,

	RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY: time.Second * 5,
	RPC_CLIENT_IDLE_CHECK_PERIOD:        time.Minute * 1,

	HEART_BEAT_INITIAL_DELAY: time.Second * 1,
	HEART_BEAT_PERIOD:        time.Second * 10,

	LOG_STATS_INITIAL_DELAY: time.Second * 60,
	LOG_STATS_PERIOD:        time.Second * 60,

	SYNC_SETTINGS_DELAY:  time.Second * 1,
	SYNC_SETTINGS_PERIOD: time.Minute * 5,
}

const heartbeatRecoveryThreshold = 2
const heartbeatRecoveryCooldown = 30 * time.Second

var errRpcEndpointsRetired = errors.New("rocketmq: RPC endpoints retired")

type rpcClientEntry struct {
	// Identity, endpoints and rpc never change. A replacement gets a new entry,
	// even if a custom factory happens to return the same RpcClient instance.
	target    string
	endpoints *v2.Endpoints
	rpc       RpcClient
	cancel    context.CancelFunc

	// Protected by rpcClientTableLock.
	deadlineFailures int
	lastRecovery     time.Time
}

type rpcClientJob struct {
	entry  *rpcClientEntry
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	err    error // published by closing done
}

type telemetryReconnectTarget struct {
	client  *defaultClient
	session *defaultClientSession
}

type defaultClientManager struct {
	rpcClientTable     map[string]*rpcClientEntry
	rpcClientTableLock sync.RWMutex
	// Jobs are independent of cache entries: retirement must not release the
	// same-address guard while an earlier candidate or Close is still running.
	rpcClientJobs    map[string]*rpcClientJob
	rpcClientFactory func(string, ...RpcClientOption) (RpcClient, error)
	clientTable      sync.Map
	ctx              context.Context
	cancel           context.CancelFunc
	done             chan struct{}
	stopped          bool
	started          bool
	shutdownOnce     sync.Once
	workers          sync.WaitGroup
	opts             clientManagerOptions
}

var _ = ClientManager(&defaultClientManager{})

var NewDefaultClientManager = func() *defaultClientManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &defaultClientManager{
		rpcClientTable:   make(map[string]*rpcClientEntry),
		rpcClientJobs:    make(map[string]*rpcClientJob),
		rpcClientFactory: NewRpcClient,
		ctx:              ctx,
		cancel:           cancel,
		done:             make(chan struct{}),
		opts:             defaultClientManagerOptions,
	}
}

func (cm *defaultClientManager) RegisterClient(client Client) {
	id := client.GetClientID()
	cm.rpcClientTableLock.Lock()
	defer cm.rpcClientTableLock.Unlock()
	if !cm.stopped {
		cm.clientTable.Store(id, client)
	}
}

func (cm *defaultClientManager) UnRegisterClient(client Client) {
	cm.clientTable.Delete(client.GetClientID())
	cm.pruneEndpoints()
}

func (cm *defaultClientManager) startUp() {
	cm.rpcClientTableLock.Lock()
	defer cm.rpcClientTableLock.Unlock()
	if cm.stopped || cm.started {
		return
	}
	cm.started = true
	cm.schedule(cm.clearIdleRpcClients, cm.opts.RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY, cm.opts.RPC_CLIENT_IDLE_CHECK_PERIOD)
	cm.schedule(cm.doHeartbeat, cm.opts.HEART_BEAT_INITIAL_DELAY, cm.opts.HEART_BEAT_PERIOD)
	cm.schedule(cm.doStats, cm.opts.LOG_STATS_INITIAL_DELAY, cm.opts.LOG_STATS_PERIOD)
	cm.schedule(cm.syncSettings, cm.opts.SYNC_SETTINGS_DELAY, cm.opts.SYNC_SETTINGS_PERIOD)
}

// Called under the lifecycle/cache lock, so Add cannot race shutdown's Wait.
func (cm *defaultClientManager) schedule(f func(), delay, period time.Duration) {
	cm.workers.Add(1)
	go func() {
		defer cm.workers.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-cm.done:
				return
			case <-timer.C:
				if cm.ctx.Err() != nil {
					return
				}
				f()
				if period <= 0 {
					return
				}
				timer.Reset(period)
			}
		}
	}()
}

func (cm *defaultClientManager) beginWork() bool {
	cm.rpcClientTableLock.Lock()
	defer cm.rpcClientTableLock.Unlock()
	if cm.stopped {
		return false
	}
	cm.workers.Add(1)
	return true
}

func (cm *defaultClientManager) entriesSnapshot() []*rpcClientEntry {
	cm.rpcClientTableLock.RLock()
	defer cm.rpcClientTableLock.RUnlock()
	entries := make([]*rpcClientEntry, 0, len(cm.rpcClientTable))
	for _, entry := range cm.rpcClientTable {
		entries = append(entries, entry)
	}
	return entries
}

func closeRpcEntry(entry *rpcClientEntry) {
	if entry.cancel != nil {
		entry.cancel()
	}
	if entry.rpc != nil {
		_ = entry.rpc.GracefulStop()
	}
}

// Retire only the observed generation. In-flight old responses cannot remove a
// new transport at the same address. Cancellation and Close stay outside locks.
func (cm *defaultClientManager) retireEntry(entry *rpcClientEntry) {
	cm.rpcClientTableLock.Lock()
	if cm.rpcClientTable[entry.target] != entry {
		cm.rpcClientTableLock.Unlock()
		return
	}
	delete(cm.rpcClientTable, entry.target)
	job := cm.rpcClientJobs[entry.target]
	cm.rpcClientTableLock.Unlock()
	if job != nil && job.entry == entry {
		job.cancel()
	}
	closeRpcEntry(entry)
}

func (cm *defaultClientManager) clearIdleRpcClients() {
	if !cm.beginWork() {
		return
	}
	defer cm.workers.Done()
	for _, entry := range cm.entriesSnapshot() {
		if entry.rpc != nil && entry.rpc.idleDuration() > cm.opts.RPC_CLIENT_MAX_IDLE_DURATION {
			cm.retireEntry(entry)
		}
	}
}

func (cm *defaultClientManager) doHeartbeat() {
	cm.clientTable.Range(func(_, value interface{}) bool {
		if client, ok := value.(*defaultClient); ok && client.isRunning() {
			client.Heartbeat()
		}
		return cm.ctx.Err() == nil
	})
}

func (cm *defaultClientManager) doStats() {
	// TODO
}

func (cm *defaultClientManager) syncSettings() {
	cm.clientTable.Range(func(_, value interface{}) bool {
		if client, ok := value.(*defaultClient); ok && client.isRunning() {
			client.trySyncSettings()
		}
		return cm.ctx.Err() == nil
	})
}

func (cm *defaultClientManager) shutdown() {
	cm.shutdownOnce.Do(func() {
		cm.rpcClientTableLock.Lock()
		cm.stopped = true
		close(cm.done)
		entries := cm.rpcClientTable
		cm.rpcClientTable = make(map[string]*rpcClientEntry)
		cm.rpcClientTableLock.Unlock()
		cm.cancel()
		for _, entry := range entries {
			closeRpcEntry(entry)
		}
		cm.workers.Wait()
	})
}

// EndpointsToString sorts its input in place. Never let it mutate a published
// route or a cache entry shared by concurrent heartbeat and telemetry calls.
func rpcEndpointsKey(endpoints *v2.Endpoints) string {
	if endpoints == nil {
		return ""
	}
	return utils.EndpointsToString(proto.Clone(endpoints).(*v2.Endpoints))
}

func rpcTarget(endpoints *v2.Endpoints) (string, error) {
	if len(endpoints.GetAddresses()) == 0 {
		return "", ErrNoAvailableEndpoints
	}
	if endpoints.GetScheme() == v2.AddressScheme_IPv4 || endpoints.GetScheme() == v2.AddressScheme_IPv6 {
		return fmt.Sprintf("%s:///%s", DefaultScheme, rpcEndpointsKey(endpoints)), nil
	}
	return utils.ParseAddress(utils.SelectAnAddress(endpoints)), nil
}

// No client/session callback is made while holding the cache lock.
func (cm *defaultClientManager) endpointUsage(endpoints *v2.Endpoints) (active, accessPoint, hasDefaultClient bool) {
	key := rpcEndpointsKey(endpoints)
	cm.clientTable.Range(func(_, value interface{}) bool {
		client, ok := value.(*defaultClient)
		if !ok {
			return true
		}
		hasDefaultClient = true
		if !client.isEndpointsDeprecated(proto.Clone(endpoints).(*v2.Endpoints)) {
			active = true
		}
		if client.isRunning() && rpcEndpointsKey(client.accessPoint) == key {
			accessPoint = true
		}
		return true
	})
	return
}

// Deprecated endpoints keep their cached transport until the pre-existing idle
// reaper retires it, matching the Java client. Only the recovery bookkeeping is
// dropped here, so a rejoined endpoint starts from zero and an in-flight
// recovery for the now-unused transport is canceled.
func (cm *defaultClientManager) pruneEndpoints() {
	if !cm.beginWork() {
		return
	}
	defer cm.workers.Done()
	for _, entry := range cm.entriesSnapshot() {
		if active, _, _ := cm.endpointUsage(entry.endpoints); active {
			continue
		}
		cm.rpcClientTableLock.Lock()
		var job *rpcClientJob
		if cm.rpcClientTable[entry.target] == entry {
			entry.deadlineFailures = 0
			entry.lastRecovery = time.Time{}
			job = cm.rpcClientJobs[entry.target]
		}
		cm.rpcClientTableLock.Unlock()
		if job != nil && job.entry == entry {
			job.cancel()
		}
	}
}

func (cm *defaultClientManager) getRpcClientContext(ctx context.Context, endpoints *v2.Endpoints) (*rpcClientEntry, error) {
	target, err := rpcTarget(endpoints)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cm.rpcClientTableLock.Lock()
		if cm.stopped {
			cm.rpcClientTableLock.Unlock()
			return nil, context.Canceled
		}
		entry := cm.rpcClientTable[target]
		if entry != nil && entry.rpc != nil {
			cm.rpcClientTableLock.Unlock()
			return entry, nil
		}
		job := cm.rpcClientJobs[target]
		if job == nil {
			entry = &rpcClientEntry{target: target, endpoints: proto.Clone(endpoints).(*v2.Endpoints)}
			cm.rpcClientTable[target] = entry
			job = cm.startRpcJobLocked(entry)
		}
		waitingInitialDial := job.entry == entry && entry != nil && entry.rpc == nil
		cm.rpcClientTableLock.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-cm.done:
			return nil, context.Canceled
		case <-job.done:
			if waitingInitialDial && job.err != nil {
				return nil, job.err
			}
		}
	}
}

func (cm *defaultClientManager) startRpcJobLocked(entry *rpcClientEntry) *rpcClientJob {
	ctx, cancel := context.WithCancel(cm.ctx)
	job := &rpcClientJob{entry: entry, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	cm.rpcClientJobs[entry.target] = job
	cm.workers.Add(1)
	go cm.runRpcJob(job)
	return job
}

func (cm *defaultClientManager) recoveryTargets(endpoints *v2.Endpoints) (bool, []telemetryReconnectTarget) {
	active := false
	var targets []telemetryReconnectTarget
	cm.clientTable.Range(func(_, value interface{}) bool {
		client, ok := value.(*defaultClient)
		if !ok || client.isEndpointsDeprecated(proto.Clone(endpoints).(*v2.Endpoints)) {
			return true
		}
		active = true
		if session := client.getSessionIfPresent(proto.Clone(endpoints).(*v2.Endpoints)); session != nil {
			targets = append(targets, telemetryReconnectTarget{client: client, session: session})
		}
		return true
	})
	return active, targets
}

func (cm *defaultClientManager) runRpcJob(job *rpcClientJob) {
	defer cm.workers.Done()
	entry := job.entry
	recovery := entry.rpc != nil
	published := false
	defer func() {
		if !published {
			job.cancel()
		}
		cm.rpcClientTableLock.Lock()
		if cm.rpcClientJobs[entry.target] == job {
			delete(cm.rpcClientJobs, entry.target)
		}
		if !recovery && cm.rpcClientTable[entry.target] == entry {
			delete(cm.rpcClientTable, entry.target)
		}
		close(job.done)
		cm.rpcClientTableLock.Unlock()
	}()

	var targets []telemetryReconnectTarget
	if recovery {
		var active bool
		active, targets = cm.recoveryTargets(entry.endpoints)
		if !active {
			job.err = errRpcEndpointsRetired
			cm.pruneEndpoints()
			return
		}
	}
	if err := job.ctx.Err(); err != nil {
		job.err = err
		return
	}
	var candidate RpcClient
	var err error
	if recreator, ok := entry.rpc.(rpcClientRecreator); recovery && ok {
		candidate, err = recreator.recreate(job.ctx)
	} else {
		candidate, err = cm.rpcClientFactory(entry.target, rpcClientContextOption{ctx: job.ctx})
	}
	if err == nil && candidate == nil {
		err = errors.New("rocketmq: RPC factory returned no client")
	}
	active, accessPoint, hasDefaultClient := cm.endpointUsage(entry.endpoints)
	cm.rpcClientTableLock.Lock()
	current := cm.rpcClientTable[entry.target] == entry
	if err == nil {
		switch {
		case cm.stopped || job.ctx.Err() != nil:
			err = context.Canceled
		case !current || (recovery && !active) || (!recovery && hasDefaultClient && !active && !accessPoint):
			err = errRpcEndpointsRetired
		default:
			cm.rpcClientTable[entry.target] = &rpcClientEntry{
				target: entry.target, endpoints: entry.endpoints, rpc: candidate,
				cancel: job.cancel, lastRecovery: entry.lastRecovery,
			}
			published = true
		}
	}
	cm.rpcClientTableLock.Unlock()
	job.err = err
	if !published {
		if candidate != nil {
			_ = candidate.GracefulStop()
		}
		return
	}
	if recovery {
		closeRpcEntry(entry)
		for _, target := range targets {
			registered, ok := cm.clientTable.Load(target.client.GetClientID())
			if ok && registered == target.client && cm.ctx.Err() == nil {
				target.client.reconnectTelemetry(proto.Clone(entry.endpoints).(*v2.Endpoints), target.session)
			}
		}
	}
}

// Ordinary unary failures are not evidence of a half-open Ready transport: only
// heartbeat accounting and explicit server commands may replace a connection.
func (cm *defaultClientManager) recordHeartbeat(entry *rpcClientEntry, err error) {
	code := status.Code(err)
	if errors.Is(err, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	ready := false
	if code == codes.Unavailable {
		if state, ok := entry.rpc.(rpcClientState); ok {
			ready = state.getState() == connectivity.Ready
		}
	}
	cm.rpcClientTableLock.Lock()
	defer cm.rpcClientTableLock.Unlock()
	// An immutable entry pins the exact RPC used by this heartbeat, not merely
	// its address. A late completion must never account against its successor.
	if cm.stopped || cm.rpcClientTable[entry.target] != entry {
		return
	}
	recover := false
	if code == codes.DeadlineExceeded {
		entry.deadlineFailures++
		recover = entry.deadlineFailures >= heartbeatRecoveryThreshold
	} else {
		entry.deadlineFailures = 0
		recover = code == codes.Unavailable && ready
	}
	if recover && cm.rpcClientJobs[entry.target] == nil && time.Since(entry.lastRecovery) >= heartbeatRecoveryCooldown {
		entry.lastRecovery = time.Now()
		cm.startRpcJobLocked(entry)
	}
}

// Server-directed recovery bypasses the cooldown, but never the in-progress
// guard. Admission is synchronous; dialing/closing never blocks a stream reader.
func (cm *defaultClientManager) reconnect(endpoints *v2.Endpoints) {
	target, err := rpcTarget(endpoints)
	if err != nil {
		return
	}
	cm.rpcClientTableLock.Lock()
	defer cm.rpcClientTableLock.Unlock()
	entry := cm.rpcClientTable[target]
	if !cm.stopped && entry != nil && entry.rpc != nil && cm.rpcClientJobs[target] == nil {
		entry.lastRecovery = time.Now()
		cm.startRpcJobLocked(entry)
	}
}

func (cm *defaultClientManager) QueryRoute(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryRouteRequest, duration time.Duration) (*v2.QueryRouteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.QueryRoute(ctx, request)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) QueryAssignments(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryAssignmentRequest, duration time.Duration) (*v2.QueryAssignmentResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.QueryAssignments(ctx, request)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.SendMessage(ctx, request)
	return ret, normalizeGrpcError(err)
}

// The stream outlives this call and its lifetime belongs to the caller's
// context, so no timeout is applied here.
func (cm *defaultClientManager) Telemetry(ctx context.Context, endpoints *v2.Endpoints, duration time.Duration) (v2.MessagingService_TelemetryClient, error) {
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	stream, err := entry.rpc.Telemetry(ctx)
	if err == nil && stream == nil {
		err = errors.New("rocketmq: no telemetry stream")
	}
	return stream, normalizeGrpcError(err)
}

func (cm *defaultClientManager) EndTransaction(ctx context.Context, endpoints *v2.Endpoints, request *v2.EndTransactionRequest, duration time.Duration) (*v2.EndTransactionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.EndTransaction(ctx, request)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) HeartBeat(ctx context.Context, endpoints *v2.Endpoints, request *v2.HeartbeatRequest, duration time.Duration) (*v2.HeartbeatResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.HeartBeat(ctx, request)
	cm.recordHeartbeat(entry, err)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) NotifyClientTermination(ctx context.Context, endpoints *v2.Endpoints, request *v2.NotifyClientTerminationRequest, duration time.Duration) (*v2.NotifyClientTerminationResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.NotifyClientTermination(ctx, request)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) ReceiveMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	stream, err := entry.rpc.ReceiveMessage(ctx, request)
	if err != nil || stream == nil {
		return nil, normalizeGrpcError(err)
	}
	return &normalizedReceiveMessageClient{MessagingService_ReceiveMessageClient: stream}, nil
}

func (cm *defaultClientManager) AckMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.AckMessageRequest, duration time.Duration) (*v2.AckMessageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.AckMessage(ctx, request)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) ChangeInvisibleDuration(ctx context.Context, endpoints *v2.Endpoints, request *v2.ChangeInvisibleDurationRequest, duration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.ChangeInvisibleDuration(ctx, request)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) ForwardMessageToDeadLetterQueue(ctx context.Context, endpoints *v2.Endpoints, request *v2.ForwardMessageToDeadLetterQueueRequest, duration time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.ForwardMessageToDeadLetterQueue(ctx, request)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) SyncLiteSubscription(ctx context.Context, endpoints *v2.Endpoints, request *v2.SyncLiteSubscriptionRequest, duration time.Duration) (*v2.SyncLiteSubscriptionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.SyncLiteSubscription(ctx, request)
	return ret, normalizeGrpcError(err)
}

func (cm *defaultClientManager) RecallMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.RecallMessageRequest, duration time.Duration) (*v2.RecallMessageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	entry, err := cm.getRpcClientContext(ctx, endpoints)
	if err != nil {
		return nil, normalizeGrpcError(err)
	}
	ret, err := entry.rpc.RecallMessage(ctx, request)
	return ret, normalizeGrpcError(err)
}

type normalizedReceiveMessageClient struct {
	v2.MessagingService_ReceiveMessageClient
}

func (stream *normalizedReceiveMessageClient) Recv() (*v2.ReceiveMessageResponse, error) {
	response, err := stream.MessagingService_ReceiveMessageClient.Recv()
	return response, normalizeGrpcError(err)
}

func (stream *normalizedReceiveMessageClient) RecvMsg(message interface{}) error {
	return normalizeGrpcError(stream.MessagingService_ReceiveMessageClient.RecvMsg(message))
}
