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

	"go.uber.org/atomic"

	"contrib.go.opencensus.io/exporter/ocagent"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
)

type meterType int

const (
	meterPublishLatency meterType = iota
	meterDeliveryLatency
	meterAwaitTime
	meterProcessTime
)

type defaultClientMeter struct {
	enabled     atomic.Bool
	endpoints   *v2.Endpoints
	ocaExporter view.Exporter
	mutex       sync.Mutex

	meter           view.Meter
	publishMeasure  *stats.Int64Measure
	deliveryMeasure *stats.Int64Measure
	awaitMeasure    *stats.Int64Measure
	processMeasure  *stats.Int64Measure
	registeredViews []*view.View
}

func newDefaultClientMeter(enabled bool, exporter view.Exporter, endpoints *v2.Endpoints, clientID string) *defaultClientMeter {
	dcm := &defaultClientMeter{
		enabled:     *atomic.NewBool(enabled),
		endpoints:   endpoints,
		ocaExporter: exporter,
	}
	if enabled {
		dcm.initPerClientResources(clientID)
	}
	return dcm
}

func (dcm *defaultClientMeter) initPerClientResources(clientID string) {
	dcm.meter = view.NewMeter()

	prefix := fmt.Sprintf("rocketmq_%s", clientID)
	dcm.publishMeasure = stats.Int64(prefix+"_publish_latency", "Publish latency in milliseconds", "ms")
	dcm.deliveryMeasure = stats.Int64(prefix+"_delivery_latency", "Time spent delivering messages from servers to clients", "ms")
	dcm.awaitMeasure = stats.Int64(prefix+"_await_time", "Client side queuing time of messages before getting processed", "ms")
	dcm.processMeasure = stats.Int64(prefix+"_process_time", "Process message time", "ms")

	dcm.registeredViews = []*view.View{
		{
			Name:        "rocketmq_send_cost_time",
			Description: "Publish latency",
			Measure:     dcm.publishMeasure,
			Aggregation: view.Distribution(1, 5, 10, 20, 50, 200, 500),
			TagKeys:     []tag.Key{topicTag, clientIdTag, invocationStatusTag},
		},
		{
			Name:        "rocketmq_delivery_latency",
			Description: "Message delivery latency",
			Measure:     dcm.deliveryMeasure,
			Aggregation: view.Distribution(1, 5, 10, 20, 50, 200, 500),
			TagKeys:     []tag.Key{topicTag, clientIdTag, consumerGroupTag},
		},
		{
			Name:        "rocketmq_await_time",
			Description: "Message await time",
			Measure:     dcm.awaitMeasure,
			Aggregation: view.Distribution(1, 5, 20, 100, 1000, 5000, 10000),
			TagKeys:     []tag.Key{topicTag, clientIdTag, consumerGroupTag},
		},
		{
			Name:        "rocketmq_process_time",
			Description: "Message process time",
			Measure:     dcm.processMeasure,
			Aggregation: view.Distribution(1, 5, 10, 100, 1000, 10000, 60000),
			TagKeys:     []tag.Key{topicTag, clientIdTag, consumerGroupTag, invocationStatusTag},
		},
	}

	dcm.meter.Start()
	if err := dcm.meter.Register(dcm.registeredViews...); err != nil {
		sugarBaseLogger.Errorf("failed to register per-client views: %v", err)
	}
}

func (dcm *defaultClientMeter) record(mt meterType, mutators []tag.Mutator, val int64) {
	if !dcm.enabled.Load() || dcm.meter == nil {
		return
	}
	var measure *stats.Int64Measure
	switch mt {
	case meterPublishLatency:
		measure = dcm.publishMeasure
	case meterDeliveryLatency:
		measure = dcm.deliveryMeasure
	case meterAwaitTime:
		measure = dcm.awaitMeasure
	case meterProcessTime:
		measure = dcm.processMeasure
	default:
		return
	}
	ctx, err := tag.New(context.Background(), mutators...)
	if err != nil {
		sugarBaseLogger.Errorf("failed to create tag map: %v", err)
		return
	}
	tagMap := tag.FromContext(ctx)
	dcm.meter.Record(tagMap, []stats.Measurement{measure.M(val)}, nil)
}

func (dcm *defaultClientMeter) shutdown() {
	if !dcm.enabled.Load() {
		return
	}
	dcm.mutex.Lock()
	defer dcm.mutex.Unlock()

	if dcm.meter != nil {
		if dcm.ocaExporter != nil {
			dcm.meter.UnregisterExporter(dcm.ocaExporter)
		}
		dcm.meter.Unregister(dcm.registeredViews...)
		dcm.meter.Stop()
		dcm.meter = nil
	}

	if dcm.ocaExporter != nil {
		if exporter, ok := dcm.ocaExporter.(*ocagent.Exporter); ok {
			if err := exporter.Stop(); err != nil {
				sugarBaseLogger.Errorf("ocExporter stop failed, err=%w", err)
			}
		}
		dcm.ocaExporter = nil
	}
	dcm.enabled.Store(false)
}

func (dcm *defaultClientMeter) start() {
	if !dcm.enabled.Load() {
		return
	}
	if dcm.meter != nil && dcm.ocaExporter != nil {
		dcm.meter.RegisterExporter(dcm.ocaExporter)
	}
}

var NewDefaultClientMeter = func(exporter view.Exporter, on bool, endpoints *v2.Endpoints, clientID string) *defaultClientMeter {
	return newDefaultClientMeter(on, exporter, endpoints, clientID)
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
	record(mt meterType, tags []tag.Mutator, val int64)
}

var _ = ClientMeterProvider(&defaultClientMeterProvider{})

type defaultClientMeterProvider struct {
	client      Client
	clientMeter *defaultClientMeter
	globalMutex sync.Mutex
}

func (dcmp *defaultClientMeterProvider) record(mt meterType, tags []tag.Mutator, val int64) {
	dcmp.clientMeter.record(mt, tags, val)
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
		dmmi.clientMeterProvider.record(meterAwaitTime,
			[]tag.Mutator{tag.Insert(topicTag, messageCommon.topic), tag.Insert(clientIdTag, clientId), tag.Insert(consumerGroupTag, consumerGroup)},
			duration.Milliseconds())
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
		dmmi.clientMeterProvider.record(meterProcessTime,
			[]tag.Mutator{tag.Insert(topicTag, messageCommon.topic), tag.Insert(clientIdTag, clientId), tag.Insert(consumerGroupTag, consumerGroup), tag.Insert(invocationStatusTag, string(invocationStatus))},
			duration.Milliseconds())
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
		dmmi.clientMeterProvider.record(meterDeliveryLatency,
			[]tag.Mutator{tag.Insert(topicTag, messageCommon.topic), tag.Insert(clientIdTag, clientId), tag.Insert(consumerGroupTag, consumerGroup)},
			latency.Milliseconds())
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
	clientId := dmmi.clientMeterProvider.getClientID()
	for _, messageCommon := range messageCommons {
		dmmi.clientMeterProvider.record(meterPublishLatency,
			[]tag.Mutator{tag.Insert(topicTag, messageCommon.topic), tag.Insert(clientIdTag, clientId), tag.Insert(invocationStatusTag, string(invocationStatus))},
			duration.Milliseconds())
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
	return dcmp.clientMeter.enabled.Load()
}
func (dcmp *defaultClientMeterProvider) getClientID() string {
	return dcmp.client.GetClientID()
}
func (dcmp *defaultClientMeterProvider) Reset(metric *v2.Metric) {
	dcmp.globalMutex.Lock()
	defer dcmp.globalMutex.Unlock()
	endpoints := metric.GetEndpoints()
	if dcmp.clientMeter.enabled.Load() && metric.GetOn() && utils.CompareEndpoints(dcmp.clientMeter.endpoints, endpoints) {
		sugarBaseLogger.Infof("metric settings is satisfied by the current message meter, clientId=%s", dcmp.client.GetClientID())
		return
	}

	if !metric.GetOn() {
		dcmp.clientMeter.shutdown()
		sugarBaseLogger.Infof("metric is off, clientId=%s", dcmp.client.GetClientID())
		dcmp.clientMeter = NewDefaultClientMeter(nil, false, nil, dcmp.client.GetClientID())
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
	dcmp.clientMeter.shutdown()
	dcmp.clientMeter = NewDefaultClientMeter(exporter, true, endpoints, dcmp.client.GetClientID())
	dcmp.clientMeter.start()
	sugarBaseLogger.Infof("metrics is on, endpoints=%v, clientId=%s", endpoints, dcmp.client.GetClientID())
}

var NewDefaultClientMeterProvider = func(client *defaultClient) ClientMeterProvider {
	cmp := &defaultClientMeterProvider{
		client:      client,
		clientMeter: NewDefaultClientMeter(nil, false, nil, "nil"),
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
