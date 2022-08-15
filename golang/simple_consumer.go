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

	Ack(ctx context.Context, messageView *MessageView) error
	Receive(ctx context.Context, maxMessageNum int32, invisibleDuration time.Duration) ([]*MessageView, error)
}

var _ = SimpleConsumer(&defaultSimpleConsumer{})

type defaultSimpleConsumer struct {
	cli *defaultClient

	consumerGroup                string
	topicIndex                   int32
	scOpts                       simpleConsumerOptions
	scSettings                   *simpleConsumerSettings
	awaitDuration                time.Duration
	subscriptionExpressions      map[string]*FilterExpression
	subTopicRouteDataResultCache sync.Map
}

func (sc *defaultSimpleConsumer) wrapReceiveMessageRequest(batchSize int, messageQueue *v2.MessageQueue, filterExpression *FilterExpression, invisibleDuration time.Duration) *v2.ReceiveMessageRequest {
	return &v2.ReceiveMessageRequest{
		Group: &v2.Resource{
			Name: sc.consumerGroup,
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
		Group: sc.scSettings.consumerGroup,
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

func (sc *defaultSimpleConsumer) GetConsumerGroup() string {
	return sc.consumerGroup
}

func (sc *defaultSimpleConsumer) receiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest, messageQueue *v2.MessageQueue, invisibleDuration time.Duration) ([]*MessageView, error) {
	var err error
	ctx = sc.cli.Sign(ctx)
	ctx, cancel := context.WithTimeout(ctx, sc.awaitDuration)
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
				sc.cli.log.Errorf("simpleConsumer recv msg err=%v", err)
				break
			}
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
			return nil, fmt.Errorf("[error] code=%d, message=%s", status.GetCode().Number(), status.GetMessage())
		}
	}
}

func (sc *defaultSimpleConsumer) Receive(ctx context.Context, maxMessageNum int32, invisibleDuration time.Duration) ([]*MessageView, error) {
	if maxMessageNum <= 0 {
		return nil, fmt.Errorf("maxMessageNum must be greater than 0")
	}
	topics := make([]string, 0, len(sc.subscriptionExpressions))
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

	filterExpression, ok := sc.subscriptionExpressions[topic]
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
	return sc.receiveMessage(ctx, request, selectMessageQueue, invisibleDuration)
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
		Group:      sc.scSettings.consumerGroup,
		ClientType: v2.ClientType_SIMPLE_CONSUMER,
	}
}

func NewSimpleConsumer(config *Config, opts ...SimpleConsumerOption) (SimpleConsumer, error) {
	scOpts := &defaultSimpleConsumerOptions
	for _, opt := range opts {
		opt.apply(scOpts)
	}
	cli, err := scOpts.clientFunc(config)
	if err != nil {
		return nil, err
	}
	sc := &defaultSimpleConsumer{
		scOpts:        *scOpts,
		cli:           cli.(*defaultClient),
		consumerGroup: config.Group,

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

		consumerGroup: &v2.Resource{
			Name: sc.consumerGroup,
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
		return fmt.Errorf("startUp err = %v, shutdown err = %v", err, err2)
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
