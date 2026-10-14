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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	innerMD "github.com/apache/rocketmq-clients/golang/v5/metadata"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/google/uuid"
	"go.uber.org/atomic"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type Client interface {
	GetClientID() string
	Sign(ctx context.Context) context.Context
	GracefulStop() error
}

type isClient interface {
	isClient()
	SetRequestTimeout(timeout time.Duration)
	wrapHeartbeatRequest() *v2.HeartbeatRequest
	onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) error
	onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) error
	IsEndpointUpdated() bool
	isRunning() bool
	getClient() *defaultClient
	getRequestTimeout() time.Duration
}
type defaultClientSession struct {
	endpoints    *v2.Endpoints
	cli          *defaultClient
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	wake         chan struct{}
	sendMu       sync.Mutex
	mu           sync.Mutex
	observer     v2.MessagingService_TelemetryClient
	streamCancel context.CancelFunc
	generation   uint64
	changed      chan struct{}
	lastError    error
}

func NewDefaultClientSession(target string, cli *defaultClient) (*defaultClientSession, error) {
	endpoints, err := utils.ParseTarget(target)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(cli.ctx)
	cs := &defaultClientSession{
		endpoints: endpoints, cli: cli, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), wake: make(chan struct{}, 1), changed: make(chan struct{}),
	}
	go cs.run()
	return cs, nil
}

func (cs *defaultClientSession) signalChanged() {
	close(cs.changed)
	cs.changed = make(chan struct{})
}

func (cs *defaultClientSession) reconnect() {
	cs.mu.Lock()
	cs.generation++
	cancel := cs.streamCancel
	cs.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	select {
	case cs.wake <- struct{}{}:
	default:
	}
}

func (cs *defaultClientSession) run() {
	defer close(cs.done)
	for cs.ctx.Err() == nil {
		ctx, cancel := context.WithCancel(cs.ctx)
		cs.mu.Lock()
		generation := cs.generation
		cs.streamCancel = cancel
		cs.mu.Unlock()
		observer, err := cs.cli.clientManager.Telemetry(cs.cli.Sign(ctx), cs.endpoints, 365*24*time.Hour)
		if err == nil {
			cs.sendMu.Lock()
			cs.mu.Lock()
			current := generation == cs.generation && cs.ctx.Err() == nil
			cs.mu.Unlock()
			if current {
				if settings := cs.cli.getSettingsCommand(); settings != nil {
					err = observer.Send(settings)
				}
			}
			cs.mu.Lock()
			if current && err == nil && generation == cs.generation && ctx.Err() == nil {
				cs.observer = observer
				cs.lastError = nil
				cs.signalChanged()
			} else if err == nil {
				err = context.Canceled
			}
			cs.mu.Unlock()
			cs.sendMu.Unlock()
			for err == nil {
				var response *v2.TelemetryCommand
				response, err = observer.Recv()
				if err != nil {
					break
				}
				cs.mu.Lock()
				current = generation == cs.generation && ctx.Err() == nil
				cs.mu.Unlock()
				if !current {
					break
				}
				if commandErr := cs.handleTelemetryCommand(response); commandErr != nil {
					cs.cli.log.Errorf("telemetry command failed: %v", commandErr)
				}
			}
		}
		// Cancellation releases Send before CloseSend takes the single-writer lock.
		cancel()
		cs.mu.Lock()
		cs.observer = nil
		cs.streamCancel = nil
		cs.lastError = err
		cs.signalChanged()
		cs.mu.Unlock()
		if observer != nil {
			cs.sendMu.Lock()
			_ = observer.CloseSend()
			cs.sendMu.Unlock()
		}
		if cs.ctx.Err() != nil {
			return
		}
		if err != nil && !cs.cli.inited.Load() && !errors.Is(err, context.Canceled) {
			cs.cli.reportStartupResult(fmt.Errorf("failed to sync settings during startup: %w", err))
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-cs.ctx.Done():
			timer.Stop()
			return
		case <-cs.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (cs *defaultClientSession) handleTelemetryCommand(response *v2.TelemetryCommand) error {
	switch c := response.GetCommand().(type) {
	case *v2.TelemetryCommand_Settings:
		return cs.cli.onSettingsCommand(cs.endpoints, c.Settings)
	case *v2.TelemetryCommand_RecoverOrphanedTransactionCommand:
		cs.cli.onRecoverOrphanedTransactionCommand(cs.endpoints, c.RecoverOrphanedTransactionCommand)
	case *v2.TelemetryCommand_VerifyMessageCommand:
		cs.cli.onVerifyMessageCommand(cs.endpoints, c.VerifyMessageCommand)
	case *v2.TelemetryCommand_PrintThreadStackTraceCommand:
		cs.cli.onPrintThreadStackTraceCommand(cs.endpoints, c.PrintThreadStackTraceCommand)
	case *v2.TelemetryCommand_ReconnectEndpointsCommand:
		cs.cli.onReconnectEndpointsCommand(cs.endpoints, c.ReconnectEndpointsCommand)
	case *v2.TelemetryCommand_NotifyUnsubscribeLiteCommand:
		cs.cli.onNotifyUnsubscribeLiteCommand(cs.endpoints, c.NotifyUnsubscribeLiteCommand)
	default:
		return fmt.Errorf("unrecognized telemetry command: %v", response.GetCommand())
	}
	return nil
}

func (cs *defaultClientSession) release() {
	cs.cancel()
	<-cs.done
}

func (cs *defaultClientSession) publish(ctx context.Context, command *v2.TelemetryCommand) error {
	waited := false
	for {
		cs.mu.Lock()
		observer, changed, streamErr := cs.observer, cs.changed, cs.lastError
		cs.mu.Unlock()
		if err := cs.ctx.Err(); err != nil {
			return err
		}
		if observer == nil {
			if streamErr != nil {
				return streamErr
			}
			waited = true
			select {
			case <-cs.ctx.Done():
				return cs.ctx.Err()
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
				continue
			}
		}
		if waited && command.GetSettings() != nil {
			return nil
		}
		cs.sendMu.Lock()
		cs.mu.Lock()
		current := observer == cs.observer && cs.ctx.Err() == nil
		cs.mu.Unlock()
		if !current {
			cs.sendMu.Unlock()
			continue
		}
		err := observer.Send(command)
		cs.sendMu.Unlock()
		return err
	}
}

type NewClientFunc func(*Config, ...ClientOption) (Client, error)

var _ = Client(&defaultClient{})

type defaultClient struct {
	log                           *internalLogger
	config                        *Config
	opts                          clientOptions
	initTopics                    []string
	settings                      ClientSettings
	accessPoint                   *v2.Endpoints
	router                        sync.Map
	clientID                      string
	clientManager                 ClientManager
	done                          chan struct{}
	ctx                           context.Context
	cancel                        context.CancelFunc
	workers                       sync.WaitGroup
	pendingTargets                map[string]int
	startupResult                 chan error
	startupOnce                   sync.Once
	reconnectFlagMu               sync.Mutex
	clientMeterProvider           ClientMeterProvider
	messageInterceptors           []MessageInterceptor
	messageInterceptorsLock       sync.RWMutex
	endpointsTelemetryClientTable map[string]*defaultClientSession
	endpointsTelemetryClientsLock sync.RWMutex
	on                            atomic.Bool
	inited                        atomic.Bool
	clientImpl                    isClient
	ReceiveReconnect              bool
	notifyUnsubscribeLiteFunc     func(*v2.NotifyUnsubscribeLiteCommand)
}

var NewClient = func(config *Config, opts ...ClientOption) (Client, error) {
	endpoints, err := utils.ParseTarget(config.Endpoint)
	if err != nil {
		return nil, err
	}
	cli := &defaultClient{
		config:                        config,
		opts:                          defaultNSOptions,
		clientID:                      utils.GenClientID(),
		accessPoint:                   endpoints,
		messageInterceptors:           make([]MessageInterceptor, 0),
		endpointsTelemetryClientTable: make(map[string]*defaultClientSession),
		on:                            *atomic.NewBool(true),
		inited:                        *atomic.NewBool(false),
	}
	cli.log = sugarBaseLogger.With("client_id", cli.clientID)
	for _, opt := range opts {
		opt.apply(&cli.opts)
	}
	cli.done = make(chan struct{})
	cli.ctx, cli.cancel = context.WithCancel(context.Background())
	cli.pendingTargets = make(map[string]int)
	cli.startupResult = make(chan error, 1)
	cli.clientMeterProvider = NewDefaultClientMeterProvider(cli)
	return cli, nil
}

var NewClientConcrete = func(config *Config, opts ...ClientOption) (*defaultClient, error) {
	endpoints, err := utils.ParseTarget(config.Endpoint)
	if err != nil {
		return nil, err
	}
	cli := &defaultClient{
		config:                        config,
		opts:                          defaultNSOptions,
		clientID:                      utils.GenClientID(),
		accessPoint:                   endpoints,
		messageInterceptors:           make([]MessageInterceptor, 0),
		endpointsTelemetryClientTable: make(map[string]*defaultClientSession),
		on:                            *atomic.NewBool(true),
		clientManager:                 &MockClientManager{},
	}
	cli.log = sugarBaseLogger.With("client_id", cli.clientID)
	for _, opt := range opts {
		opt.apply(&cli.opts)
	}
	cli.done = make(chan struct{})
	cli.ctx, cli.cancel = context.WithCancel(context.Background())
	cli.pendingTargets = make(map[string]int)
	cli.startupResult = make(chan error, 1)
	cli.clientMeterProvider = NewDefaultClientMeterProvider(cli)
	return cli, nil
}

func (cli *defaultClient) GetClientID() string {
	return cli.clientID
}

func (cli *defaultClient) getDefaultClientSession(target string) (*defaultClientSession, error) {
	endpoints, err := utils.ParseTarget(target)
	if err != nil {
		return nil, err
	}
	target = utils.EndpointsToString(endpoints)
	cli.endpointsTelemetryClientsLock.RLock()
	tc, ok := cli.endpointsTelemetryClientTable[target]
	running := cli.isRunning()
	cli.endpointsTelemetryClientsLock.RUnlock()
	if !running {
		return nil, context.Canceled
	}
	if ok {
		return tc, nil
	}
	cli.endpointsTelemetryClientsLock.Lock()
	defer cli.endpointsTelemetryClientsLock.Unlock()
	if !cli.isRunning() {
		return nil, context.Canceled
	}
	if tc, ok := cli.endpointsTelemetryClientTable[target]; ok {
		return tc, nil
	}
	tc, err = NewDefaultClientSession(target, cli)
	if err != nil {
		return nil, err
	}
	cli.endpointsTelemetryClientTable[target] = tc
	return tc, err
}

func (cli *defaultClient) registerMessageInterceptor(messageInterceptor MessageInterceptor) {
	cli.messageInterceptorsLock.Lock()
	defer cli.messageInterceptorsLock.Unlock()
	cli.messageInterceptors = append(cli.messageInterceptors, messageInterceptor)
}

func (cli *defaultClient) doBefore(hookPoint MessageHookPoints, messageCommons []*MessageCommon) error {
	cli.messageInterceptorsLock.RLocker().Lock()
	defer cli.messageInterceptorsLock.RLocker().Unlock()

	for _, interceptor := range cli.messageInterceptors {
		err := interceptor.doBefore(hookPoint, messageCommons)
		if err != nil {
			cli.log.Errorf("exception raised while intercepting message, hookPoint=%v, err=%v", hookPoint, err)
		}
	}
	return nil
}

func (cli *defaultClient) doAfter(hookPoint MessageHookPoints, messageCommons []*MessageCommon, duration time.Duration, status MessageHookPointsStatus) error {
	cli.messageInterceptorsLock.RLocker().Lock()
	defer cli.messageInterceptorsLock.RLocker().Unlock()

	for _, interceptor := range cli.messageInterceptors {
		err := interceptor.doAfter(hookPoint, messageCommons, duration, status)
		if err != nil {
			cli.log.Errorf("exception raised while intercepting message, hookPoint=%v, err=%v", hookPoint, err)
		}
	}
	return nil
}

func (cli *defaultClient) getMessageQueues(ctx context.Context, topic string) ([]*v2.MessageQueue, error) {
	item, ok := cli.router.Load(topic)
	if ok {
		if ret, ok := item.([]*v2.MessageQueue); ok {
			return ret, nil
		}
	}
	route, err := cli.queryRoute(ctx, topic, cli.opts.timeout)
	if err != nil {
		return nil, err
	}

	if err = cli.updateRoute(topic, route); err != nil {
		return nil, err
	}
	return route, nil
}

func (cli *defaultClient) updateRoute(topic string, route []*v2.MessageQueue) error {
	targets := make(map[string]bool)
	for _, queue := range route {
		targets[utils.EndpointsToString(queue.GetBroker().GetEndpoints())] = true
	}
	cli.endpointsTelemetryClientsLock.Lock()
	if !cli.isRunning() {
		cli.endpointsTelemetryClientsLock.Unlock()
		return context.Canceled
	}
	for target := range targets {
		cli.pendingTargets[target]++
	}
	cli.endpointsTelemetryClientsLock.Unlock()
	defer func() {
		cli.endpointsTelemetryClientsLock.Lock()
		for target := range targets {
			cli.pendingTargets[target]--
			if cli.pendingTargets[target] == 0 {
				delete(cli.pendingTargets, target)
			}
		}
		cli.endpointsTelemetryClientsLock.Unlock()
		cli.pruneSessions()
	}()
	for target := range targets {
		if err := cli.mustSyncSettingsToTargert(target); err != nil {
			return err
		}
	}
	cli.endpointsTelemetryClientsLock.Lock()
	defer cli.endpointsTelemetryClientsLock.Unlock()
	if !cli.isRunning() {
		return context.Canceled
	}
	cli.router.Store(topic, route)
	return nil
}

func (cli *defaultClient) isEndpointsDeprecated(endpoints *v2.Endpoints) bool {
	target := utils.EndpointsToString(endpoints)
	cli.endpointsTelemetryClientsLock.RLock()
	defer cli.endpointsTelemetryClientsLock.RUnlock()
	if !cli.isRunning() {
		return true
	}
	if cli.pendingTargets[target] > 0 {
		return false
	}
	for _, active := range cli.getTotalTargets() {
		if active == target {
			return false
		}
	}
	return true
}

func (cli *defaultClient) getSessionIfPresent(endpoints *v2.Endpoints) *defaultClientSession {
	cli.endpointsTelemetryClientsLock.RLock()
	defer cli.endpointsTelemetryClientsLock.RUnlock()
	if !cli.isRunning() {
		return nil
	}
	return cli.endpointsTelemetryClientTable[utils.EndpointsToString(endpoints)]
}

func (cli *defaultClient) reconnectTelemetry(endpoints *v2.Endpoints, expected *defaultClientSession) {
	session := cli.getSessionIfPresent(endpoints)
	if session != nil && session == expected {
		session.reconnect()
	}
}

func (cli *defaultClient) pruneSessions() {
	cli.endpointsTelemetryClientsLock.Lock()
	active := make(map[string]bool)
	for _, target := range cli.getTotalTargets() {
		active[target] = true
	}
	var retired []*defaultClientSession
	for target, session := range cli.endpointsTelemetryClientTable {
		if !active[target] && cli.pendingTargets[target] == 0 {
			delete(cli.endpointsTelemetryClientTable, target)
			session.cancel()
			retired = append(retired, session)
		}
	}
	cli.endpointsTelemetryClientsLock.Unlock()
	if cm, ok := cli.clientManager.(*defaultClientManager); ok {
		cm.pruneEndpoints()
	}
	for _, session := range retired {
		session.release()
	}
}

func (cli *defaultClient) queryRoute(ctx context.Context, topic string, duration time.Duration) ([]*v2.MessageQueue, error) {
	ctx = cli.Sign(ctx)
	response, err := cli.clientManager.QueryRoute(ctx, cli.accessPoint, cli.getQueryRouteRequest(topic), duration)
	if err != nil {
		return nil, err
	}
	if response.GetStatus().GetCode() != v2.Code_OK {
		return nil, &ErrRpcStatus{
			Code:    int32(response.Status.GetCode()),
			Message: response.GetStatus().GetMessage(),
		}
	}

	if len(response.GetMessageQueues()) == 0 {
		cli.log.Errorf("queryRoute result has no messageQueue, requestId=%s", utils.GetRequestID(ctx))
		return nil, errors.New("rocketmq: no available brokers")
	}
	return response.GetMessageQueues(), nil
}

func (cli *defaultClient) getQueryRouteRequest(topic string) *v2.QueryRouteRequest {
	return &v2.QueryRouteRequest{
		Topic: &v2.Resource{
			Name:              topic,
			ResourceNamespace: cli.config.NameSpace,
		},
		Endpoints: cli.accessPoint,
	}
}

func (cli *defaultClient) getTotalTargets() []string {
	endpoints := make([]string, 0)
	endpointsSet := make(map[string]bool)

	cli.router.Range(func(_, v interface{}) bool {
		messageQueues := v.([]*v2.MessageQueue)
		for _, messageQueue := range messageQueues {
			// Clone before the in-place sort inside EndpointsToString: the
			// published route is read from several goroutines at once.
			brokerEndpoints := proto.Clone(messageQueue.GetBroker().GetEndpoints()).(*v2.Endpoints)
			target := utils.EndpointsToString(brokerEndpoints)
			if _, ok := endpointsSet[target]; ok {
				continue
			}
			endpointsSet[target] = true
			endpoints = append(endpoints, target)
		}
		return true
	})
	return endpoints
}

func (cli *defaultClient) getSettingsCommand() *v2.TelemetryCommand {
	if cli.settings == nil {
		return nil
	}
	settings := cli.settings.toProtobuf()
	return &v2.TelemetryCommand{
		Command: &v2.TelemetryCommand_Settings{
			Settings: settings,
		},
	}
}

func (cli *defaultClient) queryAssignments(ctx context.Context, topic string, group string, duration time.Duration) (*[]*v2.Assignment, error) {
	ctx = cli.Sign(ctx)
	response, err := cli.clientManager.QueryAssignments(ctx, cli.accessPoint, cli.getQueryAssignmentRequest(topic, group), duration)
	if err != nil {
		return nil, err
	}
	if response.GetStatus().GetCode() != v2.Code_OK {
		return nil, &ErrRpcStatus{
			Code:    int32(response.Status.GetCode()),
			Message: response.GetStatus().GetMessage(),
		}
	}
	ret := response.GetAssignments()
	return &ret, nil
}

func (cli *defaultClient) getQueryAssignmentRequest(topic string, group string) *v2.QueryAssignmentRequest {
	return &v2.QueryAssignmentRequest{
		Topic: &v2.Resource{
			Name:              topic,
			ResourceNamespace: cli.config.NameSpace,
		},
		Group: &v2.Resource{
			Name:              group,
			ResourceNamespace: cli.config.NameSpace,
		},
		Endpoints: cli.accessPoint,
	}
}

func (cli *defaultClient) doHeartbeat(target string, request *v2.HeartbeatRequest) error {
	ctx := cli.Sign(cli.ctx)
	endpoints, err := utils.ParseTarget(target)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat, err=%v", err)
	}
	resp, err := cli.clientManager.HeartBeat(ctx, endpoints, request, cli.settings.GetRequestTimeout())
	if err != nil {
		return fmt.Errorf("failed to send heartbeat, endpoints=%v, err=%v, requestId=%s", endpoints, err, utils.GetRequestID(ctx))
	}
	if resp.Status.GetCode() != v2.Code_OK {
		cli.log.Errorf("failed to send heartbeat, code=%v, status message=[%s], endpoints=%v, requestId=%s", resp.Status.GetCode(), resp.Status.GetMessage(), endpoints, utils.GetRequestID(ctx))
		return &ErrRpcStatus{
			Code:    int32(resp.Status.GetCode()),
			Message: resp.GetStatus().GetMessage(),
		}
	}
	cli.log.Debugf("send heartbeat successfully, endpoints=%v", endpoints)
	switch p := cli.clientImpl.(type) {
	case *defaultProducer:
		if _, ok := p.isolated.LoadAndDelete(target); ok {
			cli.log.Infof("rejoin endpoints which is isolated before, endpoints=%v", endpoints)
		}
	default:
		// ignore
		break
	}
	return nil
}

func (cli *defaultClient) Heartbeat() {
	if !cli.isRunning() {
		return
	}
	targets := cli.getTotalTargets()
	request := cli.clientImpl.wrapHeartbeatRequest()
	for _, target := range targets {
		if err := cli.doHeartbeat(target, request); err != nil {
			cli.log.Error(err)
		}
	}
}

func (cli *defaultClient) trySyncSettings() {
	cli.log.Info("start trySyncSettings")
	command := cli.getSettingsCommand()
	targets := cli.getTotalTargets()
	for _, target := range targets {
		cli.telemeter(target, command)
	}
}

func (cli *defaultClient) mustSyncSettingsToTargert(target string) error {
	command := cli.getSettingsCommand()
	return cli.telemeter(target, command)
}

func (cli *defaultClient) telemeter(target string, command *v2.TelemetryCommand) error {
	cs, err := cli.getDefaultClientSession(target)
	if err != nil {
		cli.log.Errorf("getDefaultClientSession %s failed, err=%v", target, err)
		return err
	}
	// Bounded by the client lifetime, not by opts.timeout: establishing the
	// stream may legitimately take as long as the connection dial timeout,
	// and the supervisor reports its first attempt result promptly.
	ctx := cli.Sign(cli.ctx)
	err = cs.publish(ctx, command)
	if err != nil {
		cli.log.Errorf("telemeter to %s failed, err=%v", target, err)
		return err
	}
	cli.log.Infof("telemeter to %s success", target)
	return nil
}

func (cli *defaultClient) startUp() error {
	cli.log.Infof("begin to start the rocketmq client")
	cm := NewDefaultClientManager()
	cli.clientManager = cm
	cm.RegisterClient(cli)
	cm.startUp()

	for _, topic := range cli.initTopics {
		_, err := cli.getMessageQueues(cli.ctx, topic)
		if err != nil {
			return fmt.Errorf("failed to get topic route data result from remote during client startup, clientId=%s, topics=%v, err=%v", cli.clientID, cli.initTopics, err)
		}
	}
	f := func() {
		cli.router.Range(func(k, v interface{}) bool {
			topic := k.(string)
			newRoute, err := cli.queryRoute(cli.ctx, topic, cli.opts.timeout)
			if err != nil {
				cli.log.Errorf("scheduled queryRoute err=%v", err)
				return true
			}
			if newRoute == nil && v != nil {
				cli.log.Info("newRoute is nil, but oldRoute is not. do not update")
				return true
			}
			var oldRoute []*v2.MessageQueue
			if v != nil {
				oldRoute = v.([]*v2.MessageQueue)
			}
			if !routeEqual(oldRoute, newRoute) {
				if err := cli.updateRoute(topic, newRoute); err != nil {
					cli.log.Errorf("failed to update route: %v", err)
					return true
				}
				switch impl := cli.clientImpl.(type) {
				case *defaultProducer:
					existing, ok := impl.publishingRouteDataResultCache.Load(topic)
					if !ok {
						plb, err := NewPublishingLoadBalancer(newRoute)
						if err == nil {
							impl.publishingRouteDataResultCache.Store(topic, plb)
						}
					} else {
						impl.publishingRouteDataResultCache.Store(topic, existing.(PublishingLoadBalancer).CopyAndUpdate(newRoute))
					}
				case *defaultSimpleConsumer:
					filteredRoute := impl.filterTopicRouteData(newRoute)
					existing, ok := impl.subTopicRouteDataResultCache.Load(topic)
					if !ok {
						slb, err := NewSubscriptionLoadBalancer(filteredRoute)
						if err == nil {
							impl.subTopicRouteDataResultCache.Store(topic, slb)
						}
					} else {
						impl.subTopicRouteDataResultCache.Store(topic, existing.(SubscriptionLoadBalancer).CopyAndUpdate(filteredRoute))
					}
				}
			}
			return true
		})
	}
	cli.endpointsTelemetryClientsLock.Lock()
	if !cli.isRunning() {
		cli.endpointsTelemetryClientsLock.Unlock()
		return context.Canceled
	}
	cli.workers.Add(1)
	cli.endpointsTelemetryClientsLock.Unlock()
	go func() {
		defer cli.workers.Done()
		timer := time.NewTicker(30 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-cli.ctx.Done():
				return
			case <-timer.C:
				f()
			}
		}
	}()
	if cli.inited.Load() {
		return nil
	}
	select {
	case <-cli.ctx.Done():
		return cli.ctx.Err()
	case err := <-cli.startupResult:
		return err
	}
}

func (cli *defaultClient) reportStartupResult(err error) {
	cli.startupOnce.Do(func() { cli.startupResult <- err })
}

func routeEqual(old, new []*v2.MessageQueue) bool {
	if len(old) != len(new) {
		return false
	}
	for i := 0; i < len(old); i++ {
		if !proto.Equal(old[i], new[i]) {
			return false
		}
	}
	return true
}

func (cli *defaultClient) notifyClientTermination() {
	ctx := cli.Sign(context.Background())
	request := &v2.NotifyClientTerminationRequest{
		Group: &v2.Resource{
			ResourceNamespace: cli.config.NameSpace,
			Name:              cli.config.ConsumerGroup,
		},
	}
	targets := cli.getTotalTargets()
	for _, target := range targets {
		endpoints, _ := utils.ParseTarget(target)
		if endpoints == nil {
			continue
		}
		cli.log.Infof("start notifyClientTermination, endpoints=%s", utils.EndpointsToString(endpoints))
		_, err := cli.clientManager.NotifyClientTermination(ctx, endpoints, request, cli.opts.timeout)
		if err != nil {
			cli.log.Errorf("failed to notify client termination, endpoints=%s, error=%v", utils.EndpointsToString(endpoints), err)
		}
	}
}

func (cli *defaultClient) GracefulStop() error {
	cli.endpointsTelemetryClientsLock.Lock()
	if !cli.on.CAS(true, false) {
		cli.endpointsTelemetryClientsLock.Unlock()
		return fmt.Errorf("client has been closed")
	}
	cli.cancel()
	close(cli.done)
	sessions := cli.endpointsTelemetryClientTable
	cli.endpointsTelemetryClientTable = make(map[string]*defaultClientSession)
	cli.endpointsTelemetryClientsLock.Unlock()
	if cli.clientManager != nil {
		cli.notifyClientTermination()
		cli.clientManager.shutdown()
	}
	for _, session := range sessions {
		session.release()
	}
	cli.workers.Wait()
	cli.clientMeterProvider.Reset(&v2.Metric{On: false})
	return nil
}

func (cli *defaultClient) isRunning() bool {
	return cli.on.Load()
}

func (cli *defaultClient) Sign(ctx context.Context) context.Context {
	now := time.Now().Format("20060102T150405Z")
	if cli.config.Credentials == nil {
		// if no credentials, do not sign
		return metadata.AppendToOutgoingContext(ctx,
			innerMD.LanguageKey,
			innerMD.LanguageValue,
			innerMD.ProtocolKey,
			innerMD.ProtocolValue,
			innerMD.RequestID,
			uuid.New().String(),
			innerMD.VersionKey,
			innerMD.VersionValue,
			innerMD.ClintID,
			cli.clientID,
			innerMD.NameSpace,
			cli.config.NameSpace,
			innerMD.DateTime,
			now,
		)
	}
	return metadata.AppendToOutgoingContext(ctx,
		innerMD.LanguageKey,
		innerMD.LanguageValue,
		innerMD.ProtocolKey,
		innerMD.ProtocolValue,
		innerMD.RequestID,
		uuid.New().String(),
		innerMD.VersionKey,
		innerMD.VersionValue,
		innerMD.ClintID,
		cli.clientID,
		innerMD.NameSpace,
		cli.config.NameSpace,
		innerMD.DateTime,
		now,
		innerMD.Authorization,
		fmt.Sprintf("%s %s=%s/%s/%s, %s=%s, %s=%s",
			innerMD.EncryptHeader,
			innerMD.Credential,
			cli.config.Credentials.AccessKey,
			"",
			innerMD.Rocketmq,
			innerMD.SignedHeaders,
			innerMD.DateTime,
			innerMD.Signature,
			func() string {
				h := hmac.New(sha1.New, []byte(cli.config.Credentials.AccessSecret))
				h.Write([]byte(now))
				return hex.EncodeToString(h.Sum(nil))
			}(),
		),
	)
}

func (cli *defaultClient) onSettingsCommand(endpoints *v2.Endpoints, settings *v2.Settings) error {
	cli.log.Debugf("receive settings from remote, endpoints=%v", endpoints)
	metric := settings.GetMetric()
	if metric != nil {
		cli.clientMeterProvider.Reset(metric)
	}
	err := cli.settings.applySettingsCommand(settings)
	cli.inited.Store(true)
	cli.reportStartupResult(err)
	return err
}

func (cli *defaultClient) onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) {
	if p, ok := cli.clientImpl.(*defaultProducer); ok {
		if err := p.onRecoverOrphanedTransactionCommand(endpoints, command); err != nil {
			cli.log.Errorf("onRecoverOrphanedTransactionCommand err=%v", err)
		}
	} else {
		cli.log.Infof("ignore orphaned transaction recovery command from remote, which is not expected, command=%v", command)
	}
}

func (cli *defaultClient) onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) {
	nonce := command.GetNonce()
	status := &v2.Status{
		Code: v2.Code_NOT_IMPLEMENTED,
	}
	verifyMessageResult := &v2.VerifyMessageResult{
		Nonce: nonce,
	}
	req := &v2.TelemetryCommand{
		Status: status,
		Command: &v2.TelemetryCommand_VerifyMessageResult{
			VerifyMessageResult: verifyMessageResult,
		},
	}
	for _, address := range endpoints.GetAddresses() {
		target := utils.ParseAddress(address)
		cli.telemeter(target, req)
	}
}

func (cli *defaultClient) onNotifyUnsubscribeLiteCommand(endpoints *v2.Endpoints, command *v2.NotifyUnsubscribeLiteCommand) {
	cli.notifyUnsubscribeLiteFunc(command)
}

func (cli *defaultClient) onPrintThreadStackTraceCommand(endpoints *v2.Endpoints, command *v2.PrintThreadStackTraceCommand) {
	nonce := command.GetNonce()
	go func(nonce string) {
		// TODO get stack
		stackTrace := utils.DumpStacks()
		status := &v2.Status{
			Code: v2.Code_OK,
		}
		threadStackTrace := &v2.ThreadStackTrace{
			Nonce:            nonce,
			ThreadStackTrace: &stackTrace,
		}
		req := &v2.TelemetryCommand{
			Status: status,
			Command: &v2.TelemetryCommand_ThreadStackTrace{
				ThreadStackTrace: threadStackTrace,
			},
		}
		for _, address := range endpoints.GetAddresses() {
			target := utils.ParseAddress(address)
			cli.telemeter(target, req)
		}
	}(nonce)
}
func (cli *defaultClient) getReceiveReconnect() bool {
	cli.reconnectFlagMu.Lock()
	defer cli.reconnectFlagMu.Unlock()
	return cli.ReceiveReconnect
}

func (cli *defaultClient) setReceiveReconnect(receiveReconnect bool) {
	cli.reconnectFlagMu.Lock()
	defer cli.reconnectFlagMu.Unlock()
	cli.ReceiveReconnect = receiveReconnect
}

func (cli *defaultClient) onReconnectEndpointsCommand(endpoints *v2.Endpoints, command *v2.ReconnectEndpointsCommand) {
	cli.setReceiveReconnect(true)
	if cm, ok := cli.clientManager.(*defaultClientManager); ok {
		cm.reconnect(endpoints)
	}
}
