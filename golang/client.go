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
	"reflect"
	"sync"
	"time"

	innerMD "github.com/apache/rocketmq-clients/golang/metadata"
	"github.com/apache/rocketmq-clients/golang/pkg/ticker"
	"github.com/apache/rocketmq-clients/golang/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"github.com/google/uuid"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

type Client interface {
	GetClientID() string
	Sign(ctx context.Context) context.Context
	GracefulStop() error
}

type isClient interface {
	isClient()
	wrapHeartbeatRequest() *v2.HeartbeatRequest
	onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) error
	onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) error
}
type defaultClientSession struct {
	endpoints    *v2.Endpoints
	observer     v2.MessagingService_TelemetryClient
	observerLock sync.RWMutex
	cli          *defaultClient
	timeout      time.Duration
}

func NewDefaultClientSession(target string, cli *defaultClient) (*defaultClientSession, error) {
	endpoints, err := utils.ParseTarget(target)
	if err != nil {
		return nil, err
	}
	cs := &defaultClientSession{
		endpoints: endpoints,
		cli:       cli,
		timeout:   365 * 24 * time.Hour,
	}
	cs.startUp()
	return cs, nil
}
func (cs *defaultClientSession) startUp() {
	cs.cli.log.Infof("defaultClientSession is startUp! endpoints=%v", cs.endpoints)
	go func() {
		for {
			cs.observerLock.RLock()
			observer := cs.observer
			cs.observerLock.RUnlock()

			if observer == nil {
				time.Sleep(time.Second)
				continue
			}
			response, err := observer.Recv()
			if err != nil {
				cs.release()

				cs.cli.log.Errorf("telemetryCommand recv err=%w", err)
				continue
			}
			err = cs.handleTelemetryCommand(response)
			if err != nil {
				cs.cli.log.Errorf("telemetryCommand recv err=%w", err)
			}
		}
	}()
}
func (cs *defaultClientSession) handleTelemetryCommand(response *v2.TelemetryCommand) error {
	command := response.GetCommand()
	if command == nil {
		return fmt.Errorf("handleTelemetryCommand err = Command is nil")
	}
	switch c := command.(type) {
	case *v2.TelemetryCommand_Settings:
		cs.cli.onSettingsCommand(cs.endpoints, c.Settings)
		break
	case *v2.TelemetryCommand_RecoverOrphanedTransactionCommand:
		cs.cli.onRecoverOrphanedTransactionCommand(cs.endpoints, c.RecoverOrphanedTransactionCommand)
		break
	case *v2.TelemetryCommand_VerifyMessageCommand:
		cs.cli.onVerifyMessageCommand(cs.endpoints, c.VerifyMessageCommand)
		break
	case *v2.TelemetryCommand_PrintThreadStackTraceCommand:
		cs.cli.onPrintThreadStackTraceCommand(cs.endpoints, c.PrintThreadStackTraceCommand)
		break
	default:
		return fmt.Errorf("receive unrecognized command from remote, endpoints=%v, command=%v, clientId=%s", cs.endpoints, command, cs.cli.clientID)
	}
	return nil
}
func (cs *defaultClientSession) release() {
	cs.observerLock.Lock()
	defer cs.observerLock.Unlock()
	if err := cs.observer.CloseSend(); err != nil {
		cs.cli.log.Errorf("release defaultClientSession err=%v", err)
	}
	cs.observer = nil
}
func (cs *defaultClientSession) publish(ctx context.Context, common *v2.TelemetryCommand) error {
	var err error
	cs.observerLock.RLock()
	if cs.observer != nil {
		err = cs.observer.Send(common)
		cs.observerLock.RUnlock()
		return err
	}
	cs.observerLock.RUnlock()

	cs.observerLock.Lock()
	defer cs.observerLock.Unlock()
	if cs.observer == nil {
		tc, err := cs.cli.clientManager.Telemetry(ctx, cs.endpoints, cs.timeout)
		if err != nil {
			return err
		}
		cs.observer = tc
	}
	err = cs.observer.Send(common)
	return err
}

type NewClientFunc func(*Config, ...ClientOption) (Client, error)

var _ = Client(&defaultClient{})

type defaultClient struct {
	log                           *zap.SugaredLogger
	config                        *Config
	opts                          clientOptions
	initTopics                    []string
	settings                      ClientSettings
	accessPoint                   *v2.Endpoints
	router                        sync.Map
	clientID                      string
	clientManager                 ClientManager
	done                          chan struct{}
	clientMeterProvider           ClientMeterProvider
	messageInterceptors           []MessageInterceptor
	messageInterceptorsLock       sync.RWMutex
	endpointsTelemetryClientTable map[string]*defaultClientSession
	endpointsTelemetryClientsLock sync.RWMutex
	on                            atomic.Bool

	clientImpl isClient
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
	}
	cli.log = sugarBaseLogger.With("client_id", cli.clientID)
	for _, opt := range opts {
		opt.apply(&cli.opts)
	}
	cli.done = make(chan struct{}, 1)
	cli.clientMeterProvider = NewDefaultClientMeterProvider(cli)
	return cli, nil
}

func (cli *defaultClient) GetClientID() string {
	return cli.clientID
}

func (cli *defaultClient) getDefaultClientSession(target string) (*defaultClientSession, error) {
	cli.endpointsTelemetryClientsLock.RLock()
	tc, ok := cli.endpointsTelemetryClientTable[target]
	cli.endpointsTelemetryClientsLock.RUnlock()
	if ok {
		return tc, nil
	}
	cli.endpointsTelemetryClientsLock.Lock()
	defer cli.endpointsTelemetryClientsLock.Unlock()
	if tc, ok := cli.endpointsTelemetryClientTable[target]; ok {
		return tc, nil
	}
	tc, err := NewDefaultClientSession(target, cli)
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

func (cli *defaultClient) doBefore(hookPoint MessageHookPoints, messageCommons []*MessageCommon) {
	cli.messageInterceptorsLock.RLocker().Lock()
	defer cli.messageInterceptorsLock.RLocker().Unlock()

	for _, interceptor := range cli.messageInterceptors {
		err := interceptor.doBefore(hookPoint, messageCommons)
		if err != nil {
			cli.log.Errorf("exception raised while intercepting message, hookPoint=%v, err=%v", hookPoint, err)
		}
	}
}

func (cli *defaultClient) doAfter(hookPoint MessageHookPoints, messageCommons []*MessageCommon, duration time.Duration, status MessageHookPointsStatus) {
	cli.messageInterceptorsLock.RLocker().Lock()
	defer cli.messageInterceptorsLock.RLocker().Unlock()

	for _, interceptor := range cli.messageInterceptors {
		err := interceptor.doAfter(hookPoint, messageCommons, duration, status)
		if err != nil {
			cli.log.Errorf("exception raised while intercepting message, hookPoint=%v, err=%v", hookPoint, err)
		}
	}
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

	// telemeter to all messageQueues
	endpointsSet := make(map[string]bool)
	for _, messageQueue := range route {
		for _, address := range messageQueue.GetBroker().GetEndpoints().GetAddresses() {
			target := utils.ParseAddress(address)
			if _, ok := endpointsSet[target]; ok {
				continue
			}
			endpointsSet[target] = true
			if err = cli.mustSyncSettingsToTargert(target); err != nil {
				return nil, err
			}
		}
	}

	cli.router.Store(topic, route)
	return route, nil
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
		return nil, errors.New("rocketmq: no available brokers")
	}
	return response.GetMessageQueues(), nil
}

func (cli *defaultClient) getQueryRouteRequest(topic string) *v2.QueryRouteRequest {
	return &v2.QueryRouteRequest{
		Topic: &v2.Resource{
			Name: topic,
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
			for _, address := range messageQueue.GetBroker().GetEndpoints().GetAddresses() {
				target := utils.ParseAddress(address)
				if _, ok := endpointsSet[target]; ok {
					continue
				}
				endpointsSet[target] = true
				endpoints = append(endpoints, target)
			}
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

func (cli *defaultClient) doHeartbeat(target string, request *v2.HeartbeatRequest) error {
	ctx := cli.Sign(context.Background())
	endpoints, err := utils.ParseTarget(target)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat, err=%v", err)
	}
	resp, err := cli.clientManager.HeartBeat(ctx, endpoints, request, cli.settings.GetRequestTimeout())
	if err != nil {
		return fmt.Errorf("failed to send heartbeat, endpoints=%v, err=%v, requestId=%s", endpoints, err, utils.GetRequestID(ctx))
	}
	if resp.Status.GetCode() != v2.Code_OK {
		if resp.Status.GetCode() == v2.Code_UNRECOGNIZED_CLIENT_TYPE {
			go cli.trySyncSettings()
		}
		cli.log.Errorf("failed to send heartbeat, code=%v, status message=[%s], endpoints=%v, requestId=%s", resp.Status.GetCode(), resp.Status.GetMessage(), endpoints, utils.GetRequestID(ctx))
		return &ErrRpcStatus{
			Code:    int32(resp.Status.GetCode()),
			Message: resp.GetStatus().GetMessage(),
		}
	}
	cli.log.Infof("send heartbeat successfully, endpoints=%v", endpoints)
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
		cli.log.Error("getDefaultClientSession failed, err=%v", target, err)
		return err
	}
	ctx := cli.Sign(context.Background())
	err = cs.publish(ctx, command)
	if err != nil {
		cli.log.Error("telemeter to %s failed, err=%v", target, err)
		return err
	}
	cli.log.Infof("telemeter to %s success", target)
	return nil
}

func (cli *defaultClient) startUp() error {
	cli.log.Infof("begin to start the rocketmq client")
	cm := NewDefaultClientManager()
	cm.startUp()
	cm.RegisterClient(cli)
	cli.clientManager = cm
	for _, topic := range cli.initTopics {
		_, err := cli.getMessageQueues(context.Background(), topic)
		if err != nil {
			return fmt.Errorf("failed to get topic route data result from remote during client startup, clientId=%s, topics=%v, err=%v", cli.clientID, cli.initTopics, err)
		}
	}
	f := func() {
		cli.router.Range(func(k, v interface{}) bool {
			topic := k.(string)
			oldRoute := v
			newRoute, _ := cli.queryRoute(context.TODO(), topic, cli.opts.timeout)
			if !reflect.DeepEqual(newRoute, oldRoute) {
				cli.router.Store(k, newRoute)
				switch impl := cli.clientImpl.(type) {
				case *defaultProducer:
					plb, err := NewPublishingLoadBalancer(newRoute)
					if err == nil {
						impl.publishingRouteDataResultCache.Store(topic, plb)
					}
					break
				case *defaultSimpleConsumer:
					slb, err := NewSubscriptionLoadBalancer(newRoute)
					if err == nil {
						impl.subTopicRouteDataResultCache.Store(topic, slb)
					}
					break
				}
			}
			return true
		})
	}
	ticker.Tick(f, time.Second*30, cli.done)
	return nil
}
func (cli *defaultClient) notifyClientTermination() {
	cli.log.Info("start notifyClientTermination")
	ctx := cli.Sign(context.Background())
	request := &v2.NotifyClientTerminationRequest{}
	targets := cli.getTotalTargets()
	for _, target := range targets {
		endpoints, err := utils.ParseTarget(target)
		if err != nil {
			cli.clientManager.NotifyClientTermination(ctx, endpoints, request, cli.opts.timeout)
		}
	}
}
func (cli *defaultClient) GracefulStop() error {
	if !cli.on.CAS(true, false) {
		return fmt.Errorf("client has been closed")
	}
	cli.notifyClientTermination()
	cli.clientManager.UnRegisterClient(cli)
	cli.done <- struct{}{}
	close(cli.done)
	cli.clientMeterProvider.Reset(&v2.Metric{
		On: false,
	})
	return nil
}

func (cli *defaultClient) Sign(ctx context.Context) context.Context {
	now := time.Now().Format("20060102T150405Z")
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
	return cli.settings.applySettingsCommand(settings)
}

func (cli *defaultClient) onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) {
	if p, ok := cli.clientImpl.(*defaultProducer); ok {
		if err := p.onRecoverOrphanedTransactionCommand(endpoints, command); err != nil {
			cli.log.Errorf("onRecoverOrphanedTransactionCommand err=%w", err)
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
