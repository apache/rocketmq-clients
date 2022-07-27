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
	// "context"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/rocketmq-clients/golang/pkg/ticker"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"github.com/valyala/fastrand"
	// "time"
	// v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
)

type ClientManager interface {
	RegisterClient(client Client)
	UnRegisterClient(client Client)
	// HeartBeat(ctx context.Context, endpoints *v2.Endpoints, req *v2.HeartbeatRequest, duration time.Duration) error
	SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error)
	// QueryAssignment(ctx context.Context, endpoints *v2.Endpoints, topic string, duration time.Duration) ([]*v2.Assignment, error)
	// ReceiveMessage(ctx context.Context, endpoints *v2.Endpoints, partition *v2.MessageQueue, topic string, duration time.Duration) (v2.MessagingService_ReceiveMessageClient, error)
	// AckMessage(ctx context.Context, endpoints *v2.Endpoints, msg *MessageExt, duration time.Duration) error
}

type clientManagerOptions struct {
	RPC_CLIENT_MAX_IDLE_DURATION time.Duration

	RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY time.Duration
	RPC_CLIENT_IDLE_CHECK_PERIOD        time.Duration

	HEART_BEAT_INITIAL_DELAY time.Duration
	HEART_BEAT_PERIOD        time.Duration

	LOG_STATS_INITIAL_DELAY time.Duration
	LOG_STATS_PERIOD        time.Duration

	ANNOUNCE_SETTINGS_DELAY  time.Duration
	ANNOUNCE_SETTINGS_PERIOD time.Duration
}

var defaultClientManagerOptions = clientManagerOptions{
	RPC_CLIENT_MAX_IDLE_DURATION: time.Minute * 30,

	RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY: time.Second * 5,
	RPC_CLIENT_IDLE_CHECK_PERIOD:        time.Minute * 1,

	HEART_BEAT_INITIAL_DELAY: time.Second * 1,
	HEART_BEAT_PERIOD:        time.Second * 10,

	LOG_STATS_INITIAL_DELAY: time.Second * 60,
	LOG_STATS_PERIOD:        time.Second * 60,

	ANNOUNCE_SETTINGS_DELAY:  time.Second * 1,
	ANNOUNCE_SETTINGS_PERIOD: time.Second * 15,
}

type clientManagerImpl struct {
	rpcClientTable     map[string]RpcClient
	rpcClientTableLock sync.RWMutex
	clientTable        sync.Map
	done               chan struct{}
	opts               clientManagerOptions
}

type ClientManagerRegistry interface {
	RegisterClient(client Client) ClientManager
	UnRegisterClient(client Client) bool
}

type clientManagerRegistry struct {
	clientIds              map[string]bool
	clientIdsLock          sync.Mutex
	singletonClientManager ClientManager
}

var defaultClientManagerRegistry = &clientManagerRegistry{
	clientIds: make(map[string]bool),
}

var _ = ClientManagerRegistry(&clientManagerRegistry{})

func (cmr *clientManagerRegistry) RegisterClient(client Client) ClientManager {
	cmr.clientIdsLock.Lock()
	defer cmr.clientIdsLock.Unlock()

	if cmr.singletonClientManager == nil {
		cmr.singletonClientManager = NewClientManagerImpl()
		cmr.singletonClientManager.(*clientManagerImpl).startUp()
	}
	cmr.clientIds[client.GetClientID()] = true
	cmr.singletonClientManager.RegisterClient(client)
	return cmr.singletonClientManager
}

func (cmr *clientManagerRegistry) UnRegisterClient(client Client) bool {
	var tmpClientManager ClientManager

	cmr.clientIdsLock.Lock()
	{
		delete(cmr.clientIds, client.GetClientID())
		cmr.singletonClientManager.UnRegisterClient(client)
		if len(cmr.clientIds) == 0 {
			tmpClientManager = cmr.singletonClientManager
			cmr.singletonClientManager = nil
		}
	}
	cmr.clientIdsLock.Unlock()
	if tmpClientManager != nil {
		tmpClientManager.(*clientManagerImpl).shutdown()
	}
	return tmpClientManager != nil
}

var _ = ClientManager(&clientManagerImpl{})

func NewClientManagerImpl() *clientManagerImpl {
	return &clientManagerImpl{
		rpcClientTable: make(map[string]RpcClient),
		done:           make(chan struct{}),
		opts:           defaultClientManagerOptions,
	}
}
func (cm *clientManagerImpl) RegisterClient(client Client) {
	cm.clientTable.Store(client.GetClientID(), client)
}

func (cm *clientManagerImpl) UnRegisterClient(client Client) {
	cm.clientTable.Delete(client.GetClientID())
}

func (cm *clientManagerImpl) startUp() {
	log.Println("Begin to start the client manager")

	f := func() {
		time.Sleep(cm.opts.RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY)
		cm.clearIdleRpcClients()
	}
	ticker.Tick(f, (cm.opts.RPC_CLIENT_IDLE_CHECK_PERIOD), cm.done)

	f1 := func() {
		time.Sleep(cm.opts.HEART_BEAT_INITIAL_DELAY)
		cm.doHeartbeat()
	}
	ticker.Tick(f1, (cm.opts.HEART_BEAT_PERIOD), cm.done)

	f2 := func() {
		time.Sleep(cm.opts.LOG_STATS_INITIAL_DELAY)
		cm.doStats()
	}
	ticker.Tick(f2, (cm.opts.LOG_STATS_PERIOD), cm.done)

	f3 := func() {
		time.Sleep(cm.opts.ANNOUNCE_SETTINGS_DELAY)
		cm.syncSettings()
	}
	ticker.Tick(f3, (cm.opts.ANNOUNCE_SETTINGS_PERIOD), cm.done)

	log.Println("The client manager starts successfully")
}
func (cm *clientManagerImpl) clearIdleRpcClients() {
	log.Println("clearIdleRpcClients")
}
func (cm *clientManagerImpl) doHeartbeat() {
	log.Println("doHeartbeat")
}
func (cm *clientManagerImpl) doStats() {
	log.Println("doStats")
}
func (cm *clientManagerImpl) syncSettings() {
	log.Println("syncSettings")
}
func (cm *clientManagerImpl) shutdown() {
	log.Println("Begin to shutdown the client manager")
	close(cm.done)
	cm.done <- struct{}{}
	cm.cleanRpcClient()
	log.Println("Shutdown the client manager successfully")
}
func (cm *clientManagerImpl) cleanRpcClient() {
	log.Println("cleanRpcClient")
}
func (cm *clientManagerImpl) getRpcClient(endpoints *v2.Endpoints) (RpcClient, error) {
	addresses := endpoints.GetAddresses()
	idx := fastrand.Uint32n(uint32(len(addresses)))
	selectAddress := addresses[idx]
	target := fmt.Sprintf("%s:%d", selectAddress.Host, selectAddress.Port)

	cm.rpcClientTableLock.RLock()
	item, ok := cm.rpcClientTable[target]
	cm.rpcClientTableLock.RUnlock()
	if ok {
		if ret, ok := item.(*rpcClient); ok {
			return ret, nil
		}
	}

	cm.rpcClientTableLock.Lock()
	defer cm.rpcClientTableLock.Unlock()

	// double check
	item, ok = cm.rpcClientTable[target]
	if ok {
		if ret, ok := item.(*rpcClient); ok {
			return ret, nil
		}
	}
	rpcClient, err := NewRpcClient(target)
	if err != nil {
		return nil, err
	}
	cm.rpcClientTable[target] = rpcClient
	return rpcClient, nil
}

func (cm *clientManagerImpl) SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error) {
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	return rpcClient.SendMessage(ctx, request, duration)
}
