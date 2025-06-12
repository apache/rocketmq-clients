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
	"sync"
	"time"

	"contrib.go.opencensus.io/exporter/ocagent"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

type InvocationStatus string

const (
	InvocationStatus_SUCCESS InvocationStatus = "success"
	InvocationStatus_FAILURE InvocationStatus = "failure"
)

var (
	topicTag, _            = tag.NewKey("topic")
	clientIdTag, _         = tag.NewKey("client_id")
	invocationStatusTag, _ = tag.NewKey("invocation_status")
	consumerGroupTag, _    = tag.NewKey("consumer_group")

	PublishMLatencyMs         = stats.Int64("publish_latency", "Publish latency in milliseconds", "ms")
	ConsumeDeliveryMLatencyMs = stats.Int64("delivery_latency", "Time spent delivering messages from servers to clients", "ms")
	ConsumeAwaitMLatencyMs    = stats.Int64("await_time", "Client side queuing time of messages before getting processed", "ms")
	ConsumeProcessMLatencyMs  = stats.Int64("process_time", "Process message time", "ms")

	PublishLatencyView = view.View{
		Name:        "rocketmq_send_cost_time",
		Description: "Publish latency",
		Measure:     PublishMLatencyMs,
		Aggregation: view.Distribution(1, 5, 10, 20, 50, 200, 500),
		TagKeys:     []tag.Key{topicTag, clientIdTag, invocationStatusTag},
	}

	ConsumeDeliveryLatencyView = view.View{
		Name:        "rocketmq_delivery_latency",
		Description: "Message delivery latency",
		Measure:     ConsumeDeliveryMLatencyMs,
		Aggregation: view.Distribution(1, 5, 10, 20, 50, 200, 500),
		TagKeys:     []tag.Key{topicTag, clientIdTag, consumerGroupTag},
	}

	ConsumeAwaitTimeView = view.View{
		Name:        "rocketmq_await_time",
		Description: "Message await time",
		Measure:     ConsumeAwaitMLatencyMs,
		Aggregation: view.Distribution(1, 5, 20, 100, 1000, 5000, 10000),
		TagKeys:     []tag.Key{topicTag, clientIdTag, consumerGroupTag},
	}

	ConsumeProcessTimeView = view.View{
		Name:        "rocketmq_process_time",
		Description: "Message process time",
		Measure:     ConsumeProcessMLatencyMs,
		Aggregation: view.Distribution(1, 5, 10, 100, 1000, 10000, 60000),
		TagKeys:     []tag.Key{topicTag, clientIdTag, consumerGroupTag, invocationStatusTag},
	}
)

func init() {
	if err := view.Register(&PublishLatencyView, &ConsumeDeliveryLatencyView, &ConsumeAwaitTimeView, &ConsumeProcessTimeView); err != nil {
		sugarBaseLogger.Fatalf("failed to register views: %v", err)
	}
	view.SetReportingPeriod(time.Minute)
}

type defaultClientMeter struct {
	enabled     bool
	endpoints   *v2.Endpoints
	ocaExporter view.Exporter
	rwMutex     sync.RWMutex
}

func (dcm *defaultClientMeter) shutdown() {
	dcm.rwMutex.RLock()
	if !dcm.enabled {
		dcm.rwMutex.RUnlock()
		return
	}
	dcm.rwMutex.RUnlock()

	dcm.rwMutex.Lock()
	defer dcm.rwMutex.Unlock()
	if !dcm.enabled { // Double check
		return
	}
	dcm.enabled = false
	dcm.endpoints = nil
	view.UnregisterExporter(dcm.ocaExporter)
	exporter, ok := dcm.ocaExporter.(*ocagent.Exporter)
	if ok {
		err := exporter.Stop()
		if err != nil {
			sugarBaseLogger.Errorf("ocExporter stop failed, err=%w", err)
		}
	}
	dcm.ocaExporter = nil
}

func (dcm *defaultClientMeter) start(endpoints *v2.Endpoints, exporter view.Exporter) error {
	if endpoints == nil || exporter == nil {
		return errors.New("endpoints or exporter cannot be nil")
	}

	dcm.rwMutex.RLock()
	if dcm.enabled {
		dcm.rwMutex.RUnlock()
		return errors.New("client meter is already enabled and cannot be started again")
	}
	dcm.rwMutex.RUnlock()

	dcm.rwMutex.Lock()
	defer dcm.rwMutex.Unlock()
	if dcm.enabled { // Double check
		return errors.New("client meter is already enabled and cannot be started again")
	}
	dcm.enabled = true
	dcm.endpoints = endpoints
	dcm.ocaExporter = exporter
	view.RegisterExporter(dcm.ocaExporter)
	return nil
}

func (dcm *defaultClientMeter) isEnabled() bool {
	dcm.rwMutex.RLock()
	defer dcm.rwMutex.RUnlock()
	return dcm.enabled
}

func (dcm *defaultClientMeter) compareEndpoints(endpoints *v2.Endpoints) bool {
	dcm.rwMutex.RLock()
	defer dcm.rwMutex.RUnlock()
	if !dcm.enabled {
		return false
	}
	return utils.CompareEndpoints(dcm.endpoints, endpoints)
}

type MessageMeterInterceptor interface {
	MessageInterceptor
}

type defaultMessageMeterInterceptor struct {
	clientMeterProvider ClientMeterProvider
}

type ClientMeterProvider interface {
	Reset(metric *v2.Metric)
	isEnabled() bool
	getClientID() string
	getClientImpl() isClient
}

var _ = ClientMeterProvider(&defaultClientMeterProvider{})

type defaultClientMeterProvider struct {
	client      Client
	clientMeter *defaultClientMeter
}

func (dcmp *defaultClientMeterProvider) getClientImpl() isClient {
	if dc, ok := dcmp.client.(*defaultClient); ok {
		return dc.clientImpl
	}
	return nil
}

var _ = MessageMeterInterceptor(&defaultMessageMeterInterceptor{})

var NewDefaultMessageMeterInterceptor = func(clientMeterProvider ClientMeterProvider) *defaultMessageMeterInterceptor {
	return &defaultMessageMeterInterceptor{
		clientMeterProvider: clientMeterProvider,
	}
}

func (dmmi *defaultMessageMeterInterceptor) doBeforeConsumeMessage(messageCommons []*MessageCommon) error {
	if len(messageCommons) == 0 {
		// Should never reach here.
		return nil
	}
	clientImpl := dmmi.clientMeterProvider.getClientImpl()
	if clientImpl == nil {
		return nil
	}
	var pc PushConsumer
	var ok bool
	if pc, ok = clientImpl.(PushConsumer); !ok {
		return nil
	}
	consumerGroup := pc.GetGroupName()
	clientId := dmmi.clientMeterProvider.getClientID()
	if len(consumerGroup) == 0 {
		sugarBaseLogger.Errorf("[Bug] consumerGroup is not recognized, clientId=%s", clientId)
		return nil
	}
	for _, messageCommon := range messageCommons {
		if messageCommon.decodeStopwatch == nil {
			continue
		}
		duration := time.Since(*messageCommon.decodeStopwatch)
		err := stats.RecordWithTags(context.Background(), []tag.Mutator{tag.Insert(topicTag, messageCommon.topic), tag.Insert(clientIdTag, dmmi.clientMeterProvider.getClientID()), tag.Insert(consumerGroupTag, consumerGroup)}, ConsumeAwaitMLatencyMs.M(duration.Milliseconds()))
		if err != nil {
			return err
		}
	}

	return nil
}

func (dmmi *defaultMessageMeterInterceptor) doAfterConsumeMessage(messageCommons []*MessageCommon, duration time.Duration, status MessageHookPointsStatus) error {
	if len(messageCommons) == 0 {
		// Should never reach here.
		return nil
	}
	clientImpl := dmmi.clientMeterProvider.getClientImpl()
	if clientImpl == nil {
		return nil
	}
	var pc PushConsumer
	var ok bool
	if pc, ok = clientImpl.(PushConsumer); !ok {
		return nil
	}
	consumerGroup := pc.GetGroupName()
	clientId := dmmi.clientMeterProvider.getClientID()
	if len(consumerGroup) == 0 {
		sugarBaseLogger.Errorf("[Bug] consumerGroup is not recognized, clientId=%s", clientId)
		return nil
	}

	invocationStatus := InvocationStatus_FAILURE
	if status == MessageHookPointsStatus_OK {
		invocationStatus = InvocationStatus_SUCCESS
	}
	for _, messageCommon := range messageCommons {
		err := stats.RecordWithTags(context.Background(), []tag.Mutator{tag.Insert(topicTag, messageCommon.topic), tag.Insert(clientIdTag, dmmi.clientMeterProvider.getClientID()), tag.Insert(consumerGroupTag, consumerGroup), tag.Insert(invocationStatusTag, string(invocationStatus))}, ConsumeProcessMLatencyMs.M(duration.Milliseconds()))
		if err != nil {
			return err
		}
	}

	return nil
}

func (dmmi *defaultMessageMeterInterceptor) doAfterReceiveMessage(messageCommons []*MessageCommon, duration time.Duration, status MessageHookPointsStatus) error {
	if len(messageCommons) == 0 {
		// Should never reach here.
		return nil
	}
	clientImpl := dmmi.clientMeterProvider.getClientImpl()
	if clientImpl == nil {
		return nil
	}
	var pc PushConsumer
	var ok bool
	if pc, ok = clientImpl.(PushConsumer); !ok {
		return nil
	}
	consumerGroup := pc.GetGroupName()
	clientId := dmmi.clientMeterProvider.getClientID()
	if len(consumerGroup) == 0 {
		sugarBaseLogger.Errorf("[Bug] consumerGroup is not recognized, clientId=%s", clientId)
		return nil
	}

	for _, messageCommon := range messageCommons {
		if messageCommon.deliveryTimestamp == nil {
			continue
		}
		latency := time.Since(*messageCommon.deliveryTimestamp)
		err := stats.RecordWithTags(context.Background(), []tag.Mutator{tag.Insert(topicTag, messageCommon.topic), tag.Insert(clientIdTag, dmmi.clientMeterProvider.getClientID()), tag.Insert(consumerGroupTag, consumerGroup)}, ConsumeDeliveryMLatencyMs.M(latency.Milliseconds()))
		if err != nil {
			return err
		}
	}

	return nil
}

func (dmmi *defaultMessageMeterInterceptor) doBefore(messageHookPoints MessageHookPoints, messageCommons []*MessageCommon) error {
	if !dmmi.clientMeterProvider.isEnabled() {
		return nil
	}
	switch messageHookPoints {
	case MessageHookPoints_CONSUME:
		return dmmi.doBeforeConsumeMessage(messageCommons)
	default:
		break
	}
	return nil
}

func (dmmi *defaultMessageMeterInterceptor) doAfterSendMessage(messageCommons []*MessageCommon, duration time.Duration, status MessageHookPointsStatus) error {
	invocationStatus := InvocationStatus_FAILURE
	if status == MessageHookPointsStatus_OK {
		invocationStatus = InvocationStatus_SUCCESS
	}
	for _, messageCommon := range messageCommons {
		err := stats.RecordWithTags(context.Background(), []tag.Mutator{tag.Insert(topicTag, messageCommon.topic), tag.Insert(clientIdTag, dmmi.clientMeterProvider.getClientID()), tag.Insert(invocationStatusTag, string(invocationStatus))}, PublishMLatencyMs.M(duration.Milliseconds()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (dmmi *defaultMessageMeterInterceptor) doAfter(messageHookPoints MessageHookPoints, messageCommons []*MessageCommon, duration time.Duration, status MessageHookPointsStatus) error {
	if !dmmi.clientMeterProvider.isEnabled() {
		return nil
	}
	switch messageHookPoints {
	case MessageHookPoints_SEND:
		return dmmi.doAfterSendMessage(messageCommons, duration, status)
	case MessageHookPoints_CONSUME:
		return dmmi.doAfterConsumeMessage(messageCommons, duration, status)
	case MessageHookPoints_RECEIVE:
		return dmmi.doAfterReceiveMessage(messageCommons, duration, status)
	default:
		break
	}
	return nil
}
func (dcmp *defaultClientMeterProvider) isEnabled() bool {
	return dcmp.clientMeter.isEnabled()
}
func (dcmp *defaultClientMeterProvider) getClientID() string {
	return dcmp.client.GetClientID()
}
func (dcmp *defaultClientMeterProvider) Reset(metric *v2.Metric) {
	endpoints := metric.GetEndpoints()
	if metric.GetOn() && dcmp.clientMeter.compareEndpoints(endpoints) {
		sugarBaseLogger.Infof("metric settings is satisfied by the current message meter, clientId=%s", dcmp.client.GetClientID())
		return
	}

	dcmp.clientMeter.shutdown()
	if !metric.GetOn() {
		sugarBaseLogger.Infof("metric is off, clientId=%s", dcmp.client.GetClientID())
		return
	}

	agentAddr := utils.ParseAddress(utils.SelectAnAddress(endpoints))
	exporter, err := ocagent.NewExporter(
		ocagent.WithInsecure(),
		ocagent.WithTLSCredentials(credentials.NewTLS(defaultConnOptions.TLS)),
		ocagent.WithAddress(agentAddr),
		ocagent.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(dcmp.invokeWithSign())),
	)
	if err != nil {
		sugarBaseLogger.Errorf("exception raised when resetting message meter, clientId=%s", dcmp.client.GetClientID())
		return
	}

	// Reset message meter.
	err = dcmp.clientMeter.start(endpoints, exporter)
	if err != nil {
		sugarBaseLogger.Errorf("exception raised when resetting message meter, err=%v, clientId=%s", err, dcmp.client.GetClientID())
		return
	}
	sugarBaseLogger.Infof("metrics is on, endpoints=%v, clientId=%s", endpoints, dcmp.client.GetClientID())
}

var NewDefaultClientMeterProvider = func(client *defaultClient) ClientMeterProvider {
	cmp := &defaultClientMeterProvider{
		client:      client,
		clientMeter: &defaultClientMeter{},
	}
	client.registerMessageInterceptor(NewDefaultMessageMeterInterceptor(cmp))
	return cmp
}

var _ = ClientMeterProvider(&defaultClientMeterProvider{})

func (dcmp *defaultClientMeterProvider) invokeWithSign() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := dcmp.client.Sign(ctx)
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}
