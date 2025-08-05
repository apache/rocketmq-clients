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
	"sync"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/ticker"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
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

	CHECK_ISOLATED_ENDPOINTS_INITIAL_DELAY time.Duration
	CHECK_ISOLATED_ENDPOINTS_PERIOD        time.Duration
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

	CHECK_ISOLATED_ENDPOINTS_INITIAL_DELAY: time.Second * 1,
	CHECK_ISOLATED_ENDPOINTS_PERIOD:        time.Second * 5,
}

type defaultClientManager struct {
	rpcClientTable      map[string]RpcClient
	rpcClientTableLock  sync.RWMutex
	clientTable         sync.Map
	done                chan struct{}
	opts                clientManagerOptions
	selectedEndpointMap sync.Map
	isolated            sync.Map
}

var _ = ClientManager(&defaultClientManager{})

var NewDefaultClientManager = func() *defaultClientManager {
	return &defaultClientManager{
		rpcClientTable: make(map[string]RpcClient),
		done:           make(chan struct{}),
		opts:           defaultClientManagerOptions,
	}
}

func (cm *defaultClientManager) RegisterClient(client Client) {
	cm.clientTable.Store(client.GetClientID(), client)
}

func (cm *defaultClientManager) UnRegisterClient(client Client) {
	cm.clientTable.Delete(client.GetClientID())
}

func (cm *defaultClientManager) startUp() {
	sugarBaseLogger.Info("begin to start the client manager")

	go func() {
		time.Sleep(cm.opts.RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY)
		cm.clearIdleRpcClients()
		ticker.Tick(cm.clearIdleRpcClients, (cm.opts.RPC_CLIENT_IDLE_CHECK_PERIOD), cm.done)
	}()

	go func() {
		time.Sleep(cm.opts.HEART_BEAT_INITIAL_DELAY)
		cm.doHeartbeat()
		ticker.Tick(cm.doHeartbeat, (cm.opts.HEART_BEAT_PERIOD), cm.done)
	}()

	go func() {
		time.Sleep(cm.opts.LOG_STATS_INITIAL_DELAY)
		cm.doStats()
		ticker.Tick(cm.doStats, (cm.opts.LOG_STATS_PERIOD), cm.done)
	}()

	go func() {
		time.Sleep(cm.opts.SYNC_SETTINGS_DELAY)
		cm.syncSettings()
		ticker.Tick(cm.syncSettings, (cm.opts.SYNC_SETTINGS_PERIOD), cm.done)
	}()

	go func() {
		time.Sleep(cm.opts.CHECK_ISOLATED_ENDPOINTS_INITIAL_DELAY)
		cm.checkIsolatedEndpoints()
		ticker.Tick(cm.checkIsolatedEndpoints, (cm.opts.CHECK_ISOLATED_ENDPOINTS_PERIOD), cm.done)
	}()

	sugarBaseLogger.Info("the client manager starts successfully")
}
func (cm *defaultClientManager) deleteRpcClient(rpcClient RpcClient) {
	delete(cm.rpcClientTable, rpcClient.GetTarget())
	rpcClient.GracefulStop()
}

func (cm *defaultClientManager) clearIdleRpcClients() {
	cm.rpcClientTableLock.Lock()
	defer cm.rpcClientTableLock.Unlock()
	for target, rpcClient := range cm.rpcClientTable {
		idleDuration := rpcClient.idleDuration()
		if idleDuration > cm.opts.RPC_CLIENT_MAX_IDLE_DURATION {
			cm.deleteRpcClient(rpcClient)
			sugarBaseLogger.Warnf("rpc client has been idle for a long time, target=%s, idleDuration=%d, rpcClientMaxIdleDuration=%d\n", target, idleDuration, cm.opts.RPC_CLIENT_MAX_IDLE_DURATION)
		}
	}
}
func (cm *defaultClientManager) doHeartbeat() {
	sugarBaseLogger.Debug("clientManager start doHeartbeat")
	cm.clientTable.Range(func(_, v interface{}) bool {
		client := v.(*defaultClient)
		client.Heartbeat()
		return true
	})
}
func (cm *defaultClientManager) doStats() {
	// TODO
}
func (cm *defaultClientManager) syncSettings() {
	sugarBaseLogger.Info("clientManager start syncSettings")
	cm.clientTable.Range(func(_, v interface{}) bool {
		client := v.(*defaultClient)
		client.trySyncSettings()
		return true
	})
}
func (cm *defaultClientManager) checkIsolatedEndpoints() {
	sugarBaseLogger.Debug("clientManager start checkIsolatedEndpoints")
	cm.isolated.Range(func(key, _ interface{}) bool {
		endpoint, ok := key.(string)
		if !ok {
			return true
		}
		rpcClient, err := NewRpcClient(endpoint)
		if err == nil {
			_, err = rpcClient.HeartBeat(context.TODO(), &v2.HeartbeatRequest{})
			if err == nil {
				cm.isolated.Delete(endpoint)
			}
			rpcClient.GracefulStop()
		}
		return true
	})
}
func (cm *defaultClientManager) shutdown() {
	sugarBaseLogger.Info("begin to shutdown the client manager")
	cm.done <- struct{}{}
	close(cm.done)
	cm.cleanRpcClient()
	sugarBaseLogger.Info("shutdown the client manager successfully")
}
func (cm *defaultClientManager) cleanRpcClient() {
	sugarBaseLogger.Info("clientManager start cleanRpcClient")
	cm.rpcClientTableLock.Lock()
	defer cm.rpcClientTableLock.Unlock()
	for _, rpcClient := range cm.rpcClientTable {
		cm.deleteRpcClient(rpcClient)
	}
}
func (cm *defaultClientManager) getRpcClient(endpoints *v2.Endpoints) (RpcClient, error) {
	// Reuse endpoints to get RpcClient
	targets := utils.FormatTarget(endpoints)
	if selectedEndpoint, ok := cm.selectedEndpointMap.Load(targets); ok {
		cm.rpcClientTableLock.RLock()
		item, ok := cm.rpcClientTable[selectedEndpoint.(string)]
		cm.rpcClientTableLock.RUnlock()
		if ok {
			if ret, ok := item.(*rpcClient); ok {
				return ret, nil
			}
		}
		cm.selectedEndpointMap.Delete(targets)
	}
	// If the RpcClient for the target does not exist, create a new one
	// Filter out isolated endpoints and select a target address
	var filtered []*v2.Address
	for _, addr := range endpoints.GetAddresses() {
		parsed := utils.ParseAddress(addr)
		if _, isolated := cm.isolated.Load(parsed); !isolated {
			filtered = append(filtered, addr)
		}
	}
	// If all addresses are isolated, return an error indicating no available endpoints
	if len(filtered) == 0 {
		return nil, fmt.Errorf("no available endpoints for targets %s, all endpoints are isolated", targets)
	}
	selectedEndpoint := utils.ParseAddress(utils.SelectAnAddress(&v2.Endpoints{Addresses: filtered}))
	cm.rpcClientTableLock.RLock()
	item, ok := cm.rpcClientTable[selectedEndpoint]
	cm.rpcClientTableLock.RUnlock()
	if ok {
		if ret, ok := item.(*rpcClient); ok {
			cm.selectedEndpointMap.Store(targets, selectedEndpoint)
			return ret, nil
		}
	}
	rpcClient, err := NewRpcClient(selectedEndpoint)
	if err != nil {
		cm.isolated.Store(selectedEndpoint, true)
		sugarBaseLogger.Warnf("Create newRpcClient failed, err=%v, target=%s, trying to use another available endpoint to retry.", err, selectedEndpoint)
		return cm.getRpcClient(endpoints)
	}
	cm.selectedEndpointMap.Store(targets, selectedEndpoint)
	cm.rpcClientTable[selectedEndpoint] = rpcClient
	return rpcClient, nil
}
func (cm *defaultClientManager) handleGrpcError(rpcClient RpcClient, err error) {
	if err != nil {
		// Mark the target address of this rpcClient as isolated
		cm.isolated.Store(rpcClient.GetTarget(), true)
		sugarBaseLogger.Errorf("grpc communication error occurred. err=%v, close rpcClient=%s", err, rpcClient.GetTarget())
		cm.rpcClientTableLock.Lock()
		defer cm.rpcClientTableLock.Unlock()
		cm.deleteRpcClient(rpcClient)
	}
}
func (cm *defaultClientManager) QueryRoute(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryRouteRequest, duration time.Duration) (*v2.QueryRouteResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.QueryRoute(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("QueryRoute failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.QueryRoute(ctx, endpoints, request, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) QueryAssignments(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryAssignmentRequest, duration time.Duration) (*v2.QueryAssignmentResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.QueryAssignments(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("QueryAssignments failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.QueryAssignments(ctx, endpoints, request, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.SendMessage(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("SendMessage failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.SendMessage(ctx, endpoints, request, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) Telemetry(ctx context.Context, endpoints *v2.Endpoints, duration time.Duration) (v2.MessagingService_TelemetryClient, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.Telemetry(ctx)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("Telemetry failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.Telemetry(ctx, endpoints, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) EndTransaction(ctx context.Context, endpoints *v2.Endpoints, request *v2.EndTransactionRequest, duration time.Duration) (*v2.EndTransactionResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.EndTransaction(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("EndTransaction failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.EndTransaction(ctx, endpoints, request, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) HeartBeat(ctx context.Context, endpoints *v2.Endpoints, request *v2.HeartbeatRequest, duration time.Duration) (*v2.HeartbeatResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}

	ret, err := rpcClient.HeartBeat(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("HeartBeat failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.HeartBeat(ctx, endpoints, request, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) NotifyClientTermination(ctx context.Context, endpoints *v2.Endpoints, request *v2.NotifyClientTerminationRequest, duration time.Duration) (*v2.NotifyClientTerminationResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.NotifyClientTermination(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("NotifyClientTermination failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.NotifyClientTermination(ctx, endpoints, request, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) ReceiveMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.ReceiveMessage(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("ReceiveMessages failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.ReceiveMessage(ctx, endpoints, request)
	}
	return ret, err
}

func (cm *defaultClientManager) AckMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.AckMessageRequest, duration time.Duration) (*v2.AckMessageResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.AckMessage(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("AckMessage failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.AckMessage(ctx, endpoints, request, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) ChangeInvisibleDuration(ctx context.Context, endpoints *v2.Endpoints, request *v2.ChangeInvisibleDurationRequest, duration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.ChangeInvisibleDuration(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("ChangeInvisibleDuration failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.ChangeInvisibleDuration(ctx, endpoints, request, duration)
	}
	return ret, err
}

func (cm *defaultClientManager) ForwardMessageToDeadLetterQueue(ctx context.Context, endpoints *v2.Endpoints, request *v2.ForwardMessageToDeadLetterQueueRequest, duration time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.ForwardMessageToDeadLetterQueue(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	if err != nil {
		sugarBaseLogger.Warnf("ForwardMessageToDeadLetterQueue failed, err=%v, target=%s, trying to use another available endpoint to retry", err, rpcClient.GetTarget())
		return cm.ForwardMessageToDeadLetterQueue(ctx, endpoints, request, duration)
	}
	return ret, err
}
