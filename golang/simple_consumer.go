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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-clients/golang/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SimpleConsumer interface {
	Consumer

	Start() error
	GracefulStop() error

	Subscribe(topic string, filterExpression *FilterExpression) error
	Unsubscribe(topic string) error
	Ack(ctx context.Context, messageView *MessageView) error
	Receive(ctx context.Context, maxMessageNum int32, invisibleDuration time.Duration) ([]*MessageView, error)
	ChangeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error
	ChangeInvisibleDurationAsync(messageView *MessageView, invisibleDuration time.Duration)
}

var _ = SimpleConsumer(&defaultSimpleConsumer{})

type defaultSimpleConsumer struct {
	cli *defaultClient

	groupName                    string
	topicIndex                   int32
	scOpts                       simpleConsumerOptions
	scSettings                   *simpleConsumerSettings
	awaitDuration                time.Duration
	subscriptionExpressionsLock  sync.RWMutex
	subscriptionExpressions      map[string]*FilterExpression
	subTopicRouteDataResultCache sync.Map
}

func (sc *defaultSimpleConsumer) isOn() bool {
	return sc.cli.on.Load()
}

func (sc *defaultSimpleConsumer) changeInvisibleDuration0(messageView *MessageView, invisibleDuration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	endpoints := messageView.endpoints
	if endpoints == nil {
		return nil, fmt.Errorf("changeInvisibleDuration failed, err = the endpoints in message is nil")
	}
	messageCommons := []*MessageCommon{messageView.GetMessageCommon()}
	sc.cli.doBefore(MessageHookPoints_CHANGE_INVISIBLE_DURATION, messageCommons)

	ctx := sc.cli.Sign(context.Background())
	request := &v2.ChangeInvisibleDurationRequest{
		Topic: &v2.Resource{
			Name: messageView.GetTopic(),
		},
		Group: &v2.Resource{
			Name: sc.groupName,
		},
		ReceiptHandle:     messageView.GetReceiptHandle(),
		InvisibleDuration: durationpb.New(invisibleDuration),
		MessageId:         messageView.GetMessageId(),
	}
	watchTime := time.Now()
	resp, err := sc.cli.clientManager.ChangeInvisibleDuration(ctx, endpoints, request, sc.scSettings.requestTimeout)
	duration := time.Since(watchTime)
	messageHookPointsStatus := MessageHookPointsStatus_OK
	if err != nil {
		sc.cli.log.Errorf("exception raised during message acknowledgement, messageId=%s, endpoints=%v, requestId=%s", messageView.GetMessageId(), endpoints, utils.GetRequestID(ctx))
	} else if resp.GetStatus().GetCode() != v2.Code_OK {
		sc.cli.log.Errorf("failed to change message invisible duration, messageId=%s, endpoints=%v, code=%v, status message=[%s], requestId=%s", messageView.GetMessageId(), endpoints, resp.GetStatus().GetCode(), resp.GetStatus().GetMessage(), utils.GetRequestID(ctx))
		err = &ErrRpcStatus{
			Code:    int32(resp.Status.GetCode()),
			Message: resp.GetStatus().GetMessage(),
		}
	}
	if err != nil {
		messageHookPointsStatus = MessageHookPointsStatus_ERROR
	}
	sc.cli.doAfter(MessageHookPoints_CHANGE_INVISIBLE_DURATION, messageCommons, duration, messageHookPointsStatus)
	return resp, err
}

func (sc *defaultSimpleConsumer) changeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error {
	if messageView == nil {
		return fmt.Errorf("changeInvisibleDuration failed, err = the message is nil")
	}
	resp, err := sc.changeInvisibleDuration0(messageView, invisibleDuration)
	if resp != nil {
		messageView.ReceiptHandle = resp.ReceiptHandle
	}
	return err
}

func (sc *defaultSimpleConsumer) ChangeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error {
	if !sc.isOn() {
		return fmt.Errorf("simple consumer is not running")
	}
	return sc.changeInvisibleDuration(messageView, invisibleDuration)
}

func (sc *defaultSimpleConsumer) ChangeInvisibleDurationAsync(messageView *MessageView, invisibleDuration time.Duration) {
	if !sc.isOn() {
		sugarBaseLogger.Errorf("simple consumer is not running")
		return
	}
	go func() {
		sc.changeInvisibleDuration(messageView, invisibleDuration)
	}()
}

func (sc *defaultSimpleConsumer) Subscribe(topic string, filterExpression *FilterExpression) error {
	_, err := sc.cli.getMessageQueues(context.Background(), topic)
	if err != nil {
		sc.cli.log.Errorf("subscribe error=%v with topic %s for simpleConsumer", err, topic)
		return err
	}
	sc.subscriptionExpressionsLock.Lock()
	defer sc.subscriptionExpressionsLock.Unlock()

	sc.subscriptionExpressions[topic] = filterExpression
	return nil
}

func (sc *defaultSimpleConsumer) Unsubscribe(topic string) error {
	sc.subscriptionExpressionsLock.Lock()
	defer sc.subscriptionExpressionsLock.Unlock()

	delete(sc.subscriptionExpressions, topic)
	return nil
}

func (sc *defaultSimpleConsumer) wrapReceiveMessageRequest(batchSize int, messageQueue *v2.MessageQueue, filterExpression *FilterExpression, invisibleDuration time.Duration) *v2.ReceiveMessageRequest {
	return &v2.ReceiveMessageRequest{
		Group: &v2.Resource{
			Name: sc.groupName,
		},
		MessageQueue: messageQueue,
		FilterExpression: &v2.FilterExpression{
			Expression: filterExpression.expression,
		},
		BatchSize:         int32(batchSize),
		InvisibleDuration: durationpb.New(invisibleDuration),
		AutoRenew:         false,
	}
}

func (sc *defaultSimpleConsumer) wrapAckMessageRequest(messageView *MessageView) *v2.AckMessageRequest {
	return &v2.AckMessageRequest{
		Group: sc.scSettings.groupName,
		Topic: &v2.Resource{
			Name: messageView.GetTopic(),
		},
		Entries: []*v2.AckMessageEntry{
			{
				MessageId:     messageView.GetMessageId(),
				ReceiptHandle: messageView.GetReceiptHandle(),
			},
		},
	}
}

func (sc *defaultSimpleConsumer) GetGroupName() string {
	return sc.groupName
}

func (sc *defaultSimpleConsumer) receiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest, messageQueue *v2.MessageQueue, timeout time.Duration) ([]*MessageView, error) {
	var err error
	ctx = sc.cli.Sign(ctx)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	endpoints := messageQueue.GetBroker().GetEndpoints()
	receiveMessageClient, err := sc.cli.clientManager.ReceiveMessage(ctx, endpoints, request)
	if err != nil {
		return nil, err
	}
	done := make(chan bool, 1)

	resps := make([]*v2.ReceiveMessageResponse, 0)
	go func() {
		for {
			var resp *v2.ReceiveMessageResponse
			resp, err = receiveMessageClient.Recv()
			if err == io.EOF {
				done <- true
				defer close(done)
				break
			}
			if err != nil {
				sc.cli.log.Errorf("simpleConsumer recv msg err=%v, requestId=%s", err, utils.GetRequestID(ctx))
				break
			}
			sugarBaseLogger.Debugf("receiveMessage response: %v", resp)
			resps = append(resps, resp)
		}
		cancel()
	}()
	select {
	case <-ctx.Done():
		// timeout
		return nil, fmt.Errorf("[error] CODE=DEADLINE_EXCEEDED")
	case <-done:
		if err != nil && err != io.EOF {
			return nil, err
		}
		messageViewList := make([]*MessageView, 0)
		status := &v2.Status{
			Code:    v2.Code_INTERNAL_SERVER_ERROR,
			Message: "status was not set by server",
		}
		var deliveryTimestamp *timestamppb.Timestamp
		messageList := make([]*v2.Message, 0)
		for _, resp := range resps {
			switch r := resp.GetContent().(type) {
			case *v2.ReceiveMessageResponse_Status:
				status = r.Status
			case *v2.ReceiveMessageResponse_Message:
				messageList = append(messageList, r.Message)
			case *v2.ReceiveMessageResponse_DeliveryTimestamp:
				deliveryTimestamp = r.DeliveryTimestamp
			default:
				sc.cli.log.Warnf("[bug] not recognized content for receive message response, mq=%v, resp=%v", messageQueue, resp)
			}
		}
		for _, message := range messageList {
			messageView := fromProtobuf_MessageView2(message, messageQueue, deliveryTimestamp)
			messageViewList = append(messageViewList, messageView)
		}
		if status.GetCode() == v2.Code_OK {
			return messageViewList, nil
		} else {
			return nil, &ErrRpcStatus{
				Code:    int32(status.GetCode()),
				Message: status.GetMessage(),
			}
		}
	}
}

func (sc *defaultSimpleConsumer) Receive(ctx context.Context, maxMessageNum int32, invisibleDuration time.Duration) ([]*MessageView, error) {
	if !sc.isOn() {
		return nil, fmt.Errorf("simple consumer is not running")
	}
	if maxMessageNum <= 0 {
		return nil, fmt.Errorf("maxMessageNum must be greater than 0")
	}
	sc.subscriptionExpressionsLock.RLock()
	topics := make([]string, 0, len(sc.subscriptionExpressions))
	sc.subscriptionExpressionsLock.RUnlock()
	for k, _ := range sc.subscriptionExpressions {
		topics = append(topics, k)
	}
	// All topic is subscribed.
	if len(topics) == 0 {
		return nil, fmt.Errorf("there is no topic to receive message")
	}
	next := atomic.AddInt32(&sc.topicIndex, 1)
	idx := utils.Mod(next+1, len(topics))
	topic := topics[idx]

	sc.subscriptionExpressionsLock.RLock()
	filterExpression, ok := sc.subscriptionExpressions[topic]
	sc.subscriptionExpressionsLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no found filterExpression about topic: %s", topic)
	}
	subLoadBalancer, err := sc.getSubscriptionTopicRouteResult(ctx, topic)
	if err != nil {
		return nil, err
	}
	selectMessageQueue, err := subLoadBalancer.TakeMessageQueue()
	if err != nil {
		return nil, err
	}
	request := sc.wrapReceiveMessageRequest(int(maxMessageNum), selectMessageQueue, filterExpression, invisibleDuration)
	timeout := sc.scOpts.awaitDuration + sc.cli.opts.timeout
	return sc.receiveMessage(ctx, request, selectMessageQueue, timeout)
}

func (sc *defaultSimpleConsumer) isClient() {

}

func (sc *defaultSimpleConsumer) onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) error {
	return fmt.Errorf("Ignore orphaned transaction recovery command from remote, which is not expected, client id=%s, command=%v", sc.cli.clientID, command)
}

func (sc *defaultSimpleConsumer) onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) error {
	return nil
}

func (sc *defaultSimpleConsumer) wrapHeartbeatRequest() *v2.HeartbeatRequest {
	return &v2.HeartbeatRequest{
		Group:      sc.scSettings.groupName,
		ClientType: v2.ClientType_SIMPLE_CONSUMER,
	}
}

var NewSimpleConsumer = func(config *Config, opts ...SimpleConsumerOption) (SimpleConsumer, error) {
	copyOpt := defaultSimpleConsumerOptions
	scOpts := &copyOpt
	for _, opt := range opts {
		opt.apply(scOpts)
	}
	if len(config.ConsumerGroup) == 0 {
		return nil, fmt.Errorf("consumerGroup could not be nil")
	}
	cli, err := scOpts.clientFunc(config)
	if err != nil {
		return nil, err
	}
	sc := &defaultSimpleConsumer{
		scOpts:    *scOpts,
		cli:       cli.(*defaultClient),
		groupName: config.ConsumerGroup,

		awaitDuration:           scOpts.awaitDuration,
		subscriptionExpressions: scOpts.subscriptionExpressions,
	}
	sc.cli.initTopics = make([]string, 0)
	for topic, _ := range scOpts.subscriptionExpressions {
		sc.cli.initTopics = append(sc.cli.initTopics, topic)
	}
	endpoints, err := utils.ParseTarget(config.Endpoint)
	if err != nil {
		return nil, err
	}
	sc.scSettings = &simpleConsumerSettings{
		clientId:       sc.cli.GetClientID(),
		endpoints:      endpoints,
		clientType:     v2.ClientType_SIMPLE_CONSUMER,
		requestTimeout: sc.cli.opts.timeout,

		groupName: &v2.Resource{
			Name: sc.groupName,
		},
		longPollingTimeout:      scOpts.awaitDuration,
		subscriptionExpressions: scOpts.subscriptionExpressions,
	}
	sc.cli.settings = sc.scSettings
	sc.cli.clientImpl = sc
	return sc, nil
}

func (sc *defaultSimpleConsumer) Start() error {
	err := sc.cli.startUp()
	if err == nil {
		return nil
	}
	err2 := sc.GracefulStop()
	if err2 != nil {
		return fmt.Errorf("startUp err=%w, shutdown err=%v", err, err2)
	}
	return err
}

func (sc *defaultSimpleConsumer) GracefulStop() error {
	return sc.cli.GracefulStop()
}

func (sc *defaultSimpleConsumer) getSubscriptionTopicRouteResult(ctx context.Context, topic string) (SubscriptionLoadBalancer, error) {
	item, ok := sc.subTopicRouteDataResultCache.Load(topic)
	if ok {
		if ret, ok := item.(SubscriptionLoadBalancer); ok {
			return ret, nil
		}
	}
	route, err := sc.cli.getMessageQueues(ctx, topic)
	if err != nil {
		return nil, err
	}
	slb, err := NewSubscriptionLoadBalancer(route)
	if err != nil {
		return nil, err
	}
	sc.subTopicRouteDataResultCache.Store(topic, slb)
	return slb, nil
}

// Ack implements SimpleConsumer
func (sc *defaultSimpleConsumer) Ack(ctx context.Context, messageView *MessageView) error {
	if !sc.isOn() {
		return fmt.Errorf("simple consumer is not running")
	}
	endpoints := messageView.endpoints
	watchTime := time.Now()
	messageCommons := []*MessageCommon{messageView.GetMessageCommon()}
	sc.cli.doBefore(MessageHookPoints_ACK, messageCommons)
	request := sc.wrapAckMessageRequest(messageView)
	ctx = sc.cli.Sign(ctx)
	resp, err := sc.cli.clientManager.AckMessage(ctx, endpoints, request, sc.cli.opts.timeout)
	messageHookPointsStatus := MessageHookPointsStatus_ERROR
	duration := time.Since(watchTime)
	if err != nil {
		sc.cli.doAfter(MessageHookPoints_ACK, messageCommons, duration, messageHookPointsStatus)
		return err
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		messageHookPointsStatus = MessageHookPointsStatus_OK
	}
	sc.cli.doAfter(MessageHookPoints_ACK, messageCommons, duration, messageHookPointsStatus)
	return nil
}
