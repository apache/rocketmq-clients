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

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/patrickmn/go-cache"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PushConsumerCallback struct {
	callBackFunc func(context.Context, *MessageView) (ConsumeResult, error)
}

type PushConsumer interface {
	Consumer

	Start() error
	GracefulStop() error

	Subscribe(topic string, filterExpression *FilterExpression, callBackFunc func(context.Context, *MessageView) (ConsumeResult, error)) error
	Unsubscribe(topic string) error
	Ack(ctx context.Context, messageView *MessageView) error
	Receive(ctx context.Context, invisibleDuration time.Duration) error
	ChangeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error
	ChangeInvisibleDurationAsync(messageView *MessageView, invisibleDuration time.Duration)
}

var _ = PushConsumer(&defaultPushConsumer{})

type defaultPushConsumer struct {
	cli *defaultClient

	once                         sync.Once
	groupName                    string
	topicIndex                   int32
	pcOpts                       pushConsumerOptions
	scSettings                   *pushConsumerSettings
	awaitDuration                time.Duration
	invisibleDuration            time.Duration
	subscriptionExpressionsLock  sync.RWMutex
	subscriptionExpressions      map[string]*FilterExpression
	subTopicRouteDataResultCache sync.Map
	messageViewCacheSize         int
	messageViewCache             *cache.Cache
	consumeFuncs                 map[string]*PushConsumerCallback
}

var NewPushConsumer = func(config *Config, opts ...PushConsumerOption) (PushConsumer, error) {
	copyOpt := defaultPushConsumerOptions
	pcOpts := &copyOpt
	cOpts := pcOpts.consumerOptions
	for _, opt := range opts {
		if opt.(*FuncPushConsumerOption).FuncConsumerOption != nil {
			opt.apply(cOpts)
		}
		if opt.(*FuncPushConsumerOption).f1 != nil {
			opt.apply0(pcOpts)
		}
	}
	if len(config.ConsumerGroup) == 0 {
		return nil, fmt.Errorf("consumerGroup could not be nil")
	}
	cli, err := cOpts.clientFunc(config)
	if err != nil {
		return nil, err
	}
	pc := &defaultPushConsumer{
		pcOpts:    *pcOpts,
		cli:       cli.(*defaultClient),
		groupName: config.ConsumerGroup,

		awaitDuration:           cOpts.awaitDuration,
		invisibleDuration:       pcOpts.invisibleDuration,
		subscriptionExpressions: cOpts.subscriptionExpressions,
		messageViewCacheSize:    pcOpts.messageViewCacheSize,
		// This cache will never expire
		messageViewCache: cache.New(0, 0),
	}
	if pc.subscriptionExpressions == nil {
		pc.subscriptionExpressions = make(map[string]*FilterExpression)
	}
	pc.cli.initTopics = make([]string, 0)
	for topic, _ := range pcOpts.consumerOptions.subscriptionExpressions {
		pc.cli.initTopics = append(pc.cli.initTopics, topic)
	}
	endpoints, err := utils.ParseTarget(config.Endpoint)
	if err != nil {
		return nil, err
	}
	pc.scSettings = &pushConsumerSettings{
		clientId:       pc.cli.GetClientID(),
		endpoints:      endpoints,
		clientType:     v2.ClientType_SIMPLE_CONSUMER,
		requestTimeout: pc.cli.opts.timeout,

		groupName: &v2.Resource{
			Name: pc.groupName,
		},
		longPollingTimeout:      cOpts.awaitDuration,
		subscriptionExpressions: cOpts.subscriptionExpressions,
	}
	pc.cli.settings = pc.scSettings
	pc.cli.clientImpl = pc
	return pc, nil
}

func (pc *defaultPushConsumer) GetGroupName() string {
	return pc.groupName
}

func (pc *defaultPushConsumer) wrapReceiveMessageRequest(batchSize int, messageQueue *v2.MessageQueue, filterExpression *FilterExpression, invisibleDuration time.Duration) *v2.ReceiveMessageRequest {
	return &v2.ReceiveMessageRequest{
		Group: &v2.Resource{
			Name: pc.groupName,
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

func (pc *defaultPushConsumer) GracefulStop() error {
	return pc.cli.GracefulStop()
}

func (pc *defaultPushConsumer) Subscribe(topic string, filterExpression *FilterExpression,
	callBackFunc func(context.Context, *MessageView) (ConsumeResult, error)) error {
	_, err := pc.cli.getMessageQueues(context.Background(), topic)
	if err != nil {
		pc.cli.log.Errorf("subscribe error=%v with topic %s for simpleConsumer", err, topic)
		return err
	}
	pc.subscriptionExpressionsLock.Lock()
	defer pc.subscriptionExpressionsLock.Unlock()

	pc.subscriptionExpressions[topic] = filterExpression
	pc.consumeFuncs[topic] = &PushConsumerCallback{callBackFunc}
	return nil
}

func (pc *defaultPushConsumer) Unsubscribe(topic string) error {
	pc.subscriptionExpressionsLock.Lock()
	defer pc.subscriptionExpressionsLock.Unlock()

	delete(pc.subscriptionExpressions, topic)
	return nil
}

func (pc *defaultPushConsumer) Ack(ctx context.Context, messageView *MessageView) error {
	if !pc.isOn() {
		return fmt.Errorf("simple consumer is not running")
	}
	endpoints := messageView.endpoints
	watchTime := time.Now()
	messageCommons := []*MessageCommon{messageView.GetMessageCommon()}
	pc.cli.doBefore(MessageHookPoints_ACK, messageCommons)
	request := pc.wrapAckMessageRequest(messageView)
	ctx = pc.cli.Sign(ctx)
	resp, err := pc.cli.clientManager.AckMessage(ctx, endpoints, request, pc.cli.opts.timeout)
	messageHookPointsStatus := MessageHookPointsStatus_ERROR
	duration := time.Since(watchTime)
	if err != nil {
		pc.cli.doAfter(MessageHookPoints_ACK, messageCommons, duration, messageHookPointsStatus)
		return err
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		messageHookPointsStatus = MessageHookPointsStatus_OK
	}
	pc.cli.doAfter(MessageHookPoints_ACK, messageCommons, duration, messageHookPointsStatus)
	return nil
}

func (pc *defaultPushConsumer) isOn() bool {
	return pc.cli.on.Load()
}

func (pc *defaultPushConsumer) wrapAckMessageRequest(messageView *MessageView) *v2.AckMessageRequest {
	return &v2.AckMessageRequest{
		Group: pc.scSettings.groupName,
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

func (pc *defaultPushConsumer) ChangeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error {
	if !pc.isOn() {
		return fmt.Errorf("simple consumer is not running")
	}
	return pc.changeInvisibleDuration(messageView, invisibleDuration)
}

func (pc *defaultPushConsumer) ChangeInvisibleDurationAsync(messageView *MessageView, invisibleDuration time.Duration) {
	if !pc.isOn() {
		sugarBaseLogger.Errorf("simple consumer is not running")
		return
	}
	go func() {
		pc.changeInvisibleDuration(messageView, invisibleDuration)
	}()
}

func (pc *defaultPushConsumer) changeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error {
	if messageView == nil {
		return fmt.Errorf("changeInvisibleDuration failed, err = the message is nil")
	}
	resp, err := pc.changeInvisibleDuration0(messageView, invisibleDuration)
	if resp != nil {
		messageView.ReceiptHandle = resp.ReceiptHandle
	}
	return err
}

func (pc *defaultPushConsumer) changeInvisibleDuration0(messageView *MessageView, invisibleDuration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	endpoints := messageView.endpoints
	if endpoints == nil {
		return nil, fmt.Errorf("changeInvisibleDuration failed, err = the endpoints in message is nil")
	}
	messageCommons := []*MessageCommon{messageView.GetMessageCommon()}
	pc.cli.doBefore(MessageHookPoints_CHANGE_INVISIBLE_DURATION, messageCommons)

	ctx := pc.cli.Sign(context.Background())
	request := &v2.ChangeInvisibleDurationRequest{
		Topic: &v2.Resource{
			Name: messageView.GetTopic(),
		},
		Group: &v2.Resource{
			Name: pc.groupName,
		},
		ReceiptHandle:     messageView.GetReceiptHandle(),
		InvisibleDuration: durationpb.New(invisibleDuration),
		MessageId:         messageView.GetMessageId(),
	}
	watchTime := time.Now()
	resp, err := pc.cli.clientManager.ChangeInvisibleDuration(ctx, endpoints, request, pc.scSettings.requestTimeout)
	duration := time.Since(watchTime)
	messageHookPointsStatus := MessageHookPointsStatus_OK
	if err != nil {
		pc.cli.log.Errorf("exception raised during message acknowledgement, messageId=%s, endpoints=%v, requestId=%s", messageView.GetMessageId(), endpoints, utils.GetRequestID(ctx))
	} else if resp.GetStatus().GetCode() != v2.Code_OK {
		pc.cli.log.Errorf("failed to change message invisible duration, messageId=%s, endpoints=%v, code=%v, status message=[%s], requestId=%s", messageView.GetMessageId(), endpoints, resp.GetStatus().GetCode(), resp.GetStatus().GetMessage(), utils.GetRequestID(ctx))
		err = &ErrRpcStatus{
			Code:    int32(resp.Status.GetCode()),
			Message: resp.GetStatus().GetMessage(),
		}
	}
	if err != nil {
		messageHookPointsStatus = MessageHookPointsStatus_ERROR
	}
	pc.cli.doAfter(MessageHookPoints_CHANGE_INVISIBLE_DURATION, messageCommons, duration, messageHookPointsStatus)
	return resp, err
}

func (pc *defaultPushConsumer) isClient() {

}

func (pc *defaultPushConsumer) wrapHeartbeatRequest() *v2.HeartbeatRequest {
	return &v2.HeartbeatRequest{
		Group:      pc.scSettings.groupName,
		ClientType: v2.ClientType_SIMPLE_CONSUMER,
	}
}

func (pc *defaultPushConsumer) onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) error {
	return fmt.Errorf("Ignore orphaned transaction recovery command from remote, which is not expected, client id=%s, command=%v", pc.cli.clientID, command)
}

func (pc *defaultPushConsumer) onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) error {
	return nil
}

func (pc *defaultPushConsumer) Receive(ctx context.Context, invisibleDuration time.Duration) error {
	if !pc.isOn() {
		return fmt.Errorf("simple consumer is not running")
	}

	pc.subscriptionExpressionsLock.RLock()
	topics := make([]string, 0, len(pc.subscriptionExpressions))
	for k, _ := range pc.subscriptionExpressions {
		topics = append(topics, k)
	}
	pc.subscriptionExpressionsLock.RUnlock()

	// All topic is subscribed.
	if len(topics) == 0 {
		return fmt.Errorf("there is no topic to receive message")
	}
	next := atomic.AddInt32(&pc.topicIndex, 1)
	idx := utils.Mod(next+1, len(topics))
	topic := topics[idx]

	pc.subscriptionExpressionsLock.RLock()
	filterExpression, ok := pc.subscriptionExpressions[topic]
	pc.subscriptionExpressionsLock.RUnlock()
	if !ok {
		return fmt.Errorf("no found filterExpression about topic: %s", topic)
	}
	subLoadBalancer, err := pc.getSubscriptionTopicRouteResult(ctx, topic)
	if err != nil {
		return err
	}
	selectMessageQueue, err := subLoadBalancer.TakeMessageQueue()
	if err != nil {
		return err
	}
	request := pc.wrapReceiveMessageRequest(1, selectMessageQueue, filterExpression, invisibleDuration)
	timeout := pc.pcOpts.consumerOptions.awaitDuration + pc.cli.opts.timeout
	err = pc.receiveMessage(ctx, request, selectMessageQueue, timeout)
	return err
}

func (pc *defaultPushConsumer) getSubscriptionTopicRouteResult(ctx context.Context, topic string) (SubscriptionLoadBalancer, error) {
	item, ok := pc.subTopicRouteDataResultCache.Load(topic)
	if ok {
		if ret, ok := item.(SubscriptionLoadBalancer); ok {
			return ret, nil
		}
	}
	route, err := pc.cli.getMessageQueues(ctx, topic)
	if err != nil {
		return nil, err
	}
	slb, err := NewSubscriptionLoadBalancer(route)
	if err != nil {
		return nil, err
	}
	pc.subTopicRouteDataResultCache.Store(topic, slb)
	return slb, nil
}

func (pc *defaultPushConsumer) receiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest, messageQueue *v2.MessageQueue, timeout time.Duration) error {
	var err error
	ctx = pc.cli.Sign(ctx)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	endpoints := messageQueue.GetBroker().GetEndpoints()
	receiveMessageClient, err := pc.cli.clientManager.ReceiveMessage(ctx, endpoints, request)
	if err != nil {
		return err
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
				pc.cli.log.Errorf("simpleConsumer recv msg err=%v, requestId=%s", err, utils.GetRequestID(ctx))
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
		return fmt.Errorf("[error] CODE=DEADLINE_EXCEEDED")
	case <-done:
		if err != nil && err != io.EOF {
			return err
		}

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
				pc.cli.log.Warnf("[bug] not recognized content for receive message response, mq=%v, resp=%v", messageQueue, resp)
			}
		}
		for _, message := range messageList {
			messageView := fromProtobuf_MessageView2(message, messageQueue, deliveryTimestamp)
			pc.messageViewCache.Set(messageView.topic+messageView.messageId, messageView, -1)
		}
		if status.GetCode() == v2.Code_OK {
			return nil
		} else {
			return &ErrRpcStatus{
				Code:    int32(status.GetCode()),
				Message: status.GetMessage(),
			}
		}
	}
}

func (pc *defaultPushConsumer) consumeInner(ctx context.Context, mv *MessageView) (ConsumeResult, error) {
	f, exist := pc.consumeFuncs[mv.topic]

	if !exist {
		return FAILURE, fmt.Errorf("the consume callback missing for topic: %s", mv.topic)
	}

	return f.callBackFunc(ctx, mv)

}

func (pc *defaultPushConsumer) Start() error {
	err := pc.cli.startUp()
	if err != nil {
		err2 := pc.GracefulStop()
		if err2 != nil {
			return fmt.Errorf("startUp err=%w, shutdown err=%v", err, err2)
		}
		return err
	}

	ctx := pc.cli.Sign(context.Background())

	// Receive data and store into cache
	go func() {
		fmt.Println("start receive message")
		for {
			if pc.messageViewCacheSize <= pc.messageViewCache.ItemCount() {
				// wait for 1 second when the cache is full
				time.Sleep(1 * time.Second)
				continue
			}
			err := pc.Receive(ctx, pc.invisibleDuration)
			if err != nil {
				fmt.Println(err)
				break
			}

		}
	}()

	// Message Consumption in Push Consumer
	go func() {
		for {
			for key, item := range pc.messageViewCache.Items() {
				// take message from cache
				mv := item.Object.(*MessageView)
				// consume message
				consumeResult, err := pc.consumeInner(ctx, mv)
				if err != nil {
					break
				}
				// erase message from cache
				pc.messageViewCache.Delete(key)

				if consumeResult == Success {
					// ack message
					err := pc.Ack(ctx, mv)
					if err != nil {
						pc.cli.log.Errorf("%s", err)
						break
					}
				} else {
					// change message invisible duration
					err := pc.changeInvisibleDuration(mv, pc.invisibleDuration)
					if err != nil {
						pc.cli.log.Errorf("%s", err)
						break
					}
				}
			}
		}
	}()

	return err
}
