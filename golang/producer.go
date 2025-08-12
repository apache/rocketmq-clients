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
	"math"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Producer interface {
	Send(context.Context, *Message) ([]*SendReceipt, error)
	SendWithTransaction(context.Context, *Message, Transaction) ([]*SendReceipt, error)
	SendAsync(context.Context, *Message, func(context.Context, []*SendReceipt, error))
	BeginTransaction() Transaction
	Start() error
	GracefulStop() error
	isClient
}

type defaultProducer struct {
	po       producerOptions
	pSetting *producerSettings
	cli      *defaultClient

	checker                        *TransactionChecker
	isolated                       sync.Map
	publishingRouteDataResultCache sync.Map
}

func (p *defaultProducer) Start() error {
	err := p.cli.startUp()
	if err == nil {
		return nil
	}
	err2 := p.GracefulStop()
	if err2 != nil {
		return fmt.Errorf("startUp err=%w, shutdown err=%v", err, err2)
	}
	return fmt.Errorf("startUp err=%w", err)
}

var _ = Producer(&defaultProducer{})

func (p *defaultProducer) isClient() {
}

func (p *defaultProducer) isOn() bool {
	return p.cli.on.Load()
}

func (p *defaultProducer) wrapHeartbeatRequest() *v2.HeartbeatRequest {
	return &v2.HeartbeatRequest{
		ClientType: v2.ClientType_PRODUCER,
	}
}

func (p *defaultProducer) takeMessageQueues(plb PublishingLoadBalancer) ([]*v2.MessageQueue, error) {
	return plb.TakeMessageQueues(&p.isolated, p.getRetryMaxAttempts())
}

func (p *defaultProducer) getPublishingTopicRouteResult(ctx context.Context, topic string) (PublishingLoadBalancer, error) {
	item, ok := p.publishingRouteDataResultCache.Load(topic)
	if ok {
		if ret, ok := item.(PublishingLoadBalancer); ok {
			return ret, nil
		}
	}
	route, err := p.cli.getMessageQueues(ctx, topic)
	if err != nil {
		return nil, err
	}
	plb, err := NewPublishingLoadBalancer(route)
	if err != nil {
		return nil, err
	}
	p.publishingRouteDataResultCache.Store(topic, plb)
	return plb, nil
}

func (p *defaultProducer) wrapSendMessageRequest(pMsgs []*PublishingMessage) (*v2.SendMessageRequest, error) {
	smr := &v2.SendMessageRequest{
		Messages: []*v2.Message{},
	}
	for _, pMsg := range pMsgs {
		msgV2, err := pMsg.toProtobuf()
		if err != nil {
			return nil, fmt.Errorf("wrapSendMessageRequest failed, {%v}", err)
		}
		smr.Messages = append(smr.Messages, msgV2)
	}
	return smr, nil
}

var NewProducer = func(config *Config, opts ...ProducerOption) (Producer, error) {
	copyOpt := defaultProducerOptions
	po := &copyOpt
	for _, opt := range opts {
		opt.apply(po)
	}
	cli, err := po.clientFunc(config)
	if err != nil {
		return nil, err
	}
	p := &defaultProducer{
		po:      *po,
		cli:     cli.(*defaultClient),
		checker: po.checker,
	}
	p.cli.initTopics = po.topics
	endpoints, err := utils.ParseTarget(config.Endpoint)
	if err != nil {
		return nil, err
	}
	p.pSetting = &producerSettings{
		clientId:   p.cli.GetClientID(),
		endpoints:  endpoints,
		clientType: v2.ClientType_PRODUCER,
		retryPolicy: &v2.RetryPolicy{
			MaxAttempts: po.maxAttempts,
			Strategy: &v2.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &v2.ExponentialBackoff{
					Max:        durationpb.New(time.Duration(0)),
					Initial:    durationpb.New(time.Duration(0)),
					Multiplier: 1,
				},
			},
		},
		requestTimeout:      p.cli.opts.timeout,
		validateMessageType: *atomic.NewBool(true),
		maxBodySizeBytes:    *atomic.NewInt32(4 * 1024 * 1024),
	}
	for _, topic := range po.topics {
		topicResource := &v2.Resource{
			Name:              topic,
			ResourceNamespace: config.NameSpace,
		}
		p.pSetting.topics.Store(topic, topicResource)
	}
	p.cli.settings = p.pSetting
	p.cli.clientImpl = p
	return p, nil
}

func (p *defaultProducer) getRetryMaxAttempts() int {
	return int(p.pSetting.GetRetryPolicy().GetMaxAttempts())
}
func (p *defaultProducer) getNextAttemptDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return time.Duration(0)
	}
	retryPolicy := p.pSetting.GetRetryPolicy().Strategy.(*v2.RetryPolicy_ExponentialBackoff).ExponentialBackoff
	delayNanos := uint32(math.Min(float64(retryPolicy.Initial.AsDuration().Nanoseconds())*math.Pow(float64(retryPolicy.Multiplier), 1.0*float64(attempt-1)), float64(retryPolicy.Max.AsDuration().Nanoseconds())))
	if delayNanos <= 0 {
		return time.Duration(0)
	}
	return time.Duration(delayNanos)
}

func (p *defaultProducer) send1(ctx context.Context, topic string, messageType v2.MessageType,
	candidates []*v2.MessageQueue, pubMessages []*PublishingMessage, attempt int) ([]*SendReceipt, error) {

	ctx = p.cli.Sign(ctx)

	idx := utils.Mod(int32(attempt)-1, len(candidates))
	selectMessageQueue := candidates[idx]

	if p.pSetting.IsValidateMessageType() && !utils.MatchMessageType(selectMessageQueue, messageType) {
		return nil, fmt.Errorf("current message type not match with topic accept message types")
	}

	endpoints := selectMessageQueue.GetBroker().GetEndpoints()

	sendReq, err := p.wrapSendMessageRequest(pubMessages)
	if err != nil {
		return nil, err
	}
	messageCommons := make([]*MessageCommon, 0)
	for _, pubMessage := range pubMessages {
		messageCommons = append(messageCommons, pubMessage.msg.GetMessageCommon())
	}
	p.cli.doBefore(MessageHookPoints_SEND, messageCommons)
	watchTime := time.Now()
	resp, err := p.cli.clientManager.SendMessage(ctx, endpoints, sendReq, p.pSetting.GetRequestTimeout())
	duration := time.Since(watchTime)
	messageHookPointsStatus := MessageHookPointsStatus_OK
	// processSendResponse
	tooManyRequests := false
	if err == nil && resp.GetStatus().GetCode() != v2.Code_OK {
		tooManyRequests = resp.GetStatus().GetCode() == v2.Code_TOO_MANY_REQUESTS
		err = &ErrRpcStatus{
			Code:    int32(resp.Status.GetCode()),
			Message: resp.GetStatus().GetMessage(),
		}
	}
	if err != nil {
		messageHookPointsStatus = MessageHookPointsStatus_ERROR
	}
	p.cli.doAfter(MessageHookPoints_SEND, messageCommons, duration, messageHookPointsStatus)
	maxAttempts := p.getRetryMaxAttempts()
	if err != nil {
		messageIds := make([]string, 0)
		for _, pubMessage := range pubMessages {
			messageIds = append(messageIds, pubMessage.messageId)
		}
		// retry
		for _, address := range endpoints.GetAddresses() {
			p.isolated.Store(utils.ParseAddress(address), true)
		}
		if attempt >= maxAttempts {
			p.cli.log.Errorf("failed to send message(s) finally, run out of attempt times, topic=%s, messageId(s)=%v, maxAttempts=%d, attempt=%d, endpoints=%v, requestId=%s",
				topic, messageIds, maxAttempts, attempt, endpoints, utils.GetRequestID(ctx))
			return nil, err
		}
		// Try to do more attempts.
		nextAttempt := attempt + 1
		// Retry immediately if the request is not throttled.
		if tooManyRequests {
			waitTime := p.getNextAttemptDelay(nextAttempt)
			p.cli.log.Warnf("failed to send message due to too many requests, would attempt to resend after %v, topic=%s, messageId(s)=%v, maxAttempts=%d, attempt=%d, endpoints=%v, requestId=%s",
				waitTime, topic, messageIds, maxAttempts, attempt, endpoints, utils.GetRequestID(ctx))
			time.Sleep(waitTime)
		} else {
			p.cli.log.Warnf("failed to send message, would attempt to resend right now, topic=%s, messageId(s)=%v, maxAttempts=%d, attempt=%d, endpoints=%v, requestId=%s",
				topic, messageIds, maxAttempts, attempt, endpoints, utils.GetRequestID(ctx))
		}
		return p.send1(ctx, topic, messageType, candidates, pubMessages, nextAttempt)
	}

	var res []*SendReceipt
	for i := 0; i < len(resp.GetEntries()); i++ {
		res = append(res, &SendReceipt{
			MessageID:     resp.GetEntries()[i].GetMessageId(),
			TransactionId: resp.GetEntries()[i].GetTransactionId(),
			Offset:        resp.GetEntries()[i].GetOffset(),
			Endpoints:     endpoints,
		})
	}
	if attempt > 1 {
		p.cli.log.Infof("resend message successfully, topic=%s, maxAttempts=%d, attempt=%d, endpoints=%s",
			topic, maxAttempts, attempt, endpoints.String())
	}
	return res, nil
}

func (p *defaultProducer) send0(ctx context.Context, msgs []*UnifiedMessage, txEnabled bool) ([]*SendReceipt, error) {
	// check topic Name
	topicName := msgs[0].GetMessage().Topic
	for _, msg := range msgs {
		if msg.GetMessage().Topic != topicName {
			return nil, fmt.Errorf("messages to send have different topics")
		}
	}

	pubMessages := make([]*PublishingMessage, len(msgs))
	for idx, uMsg := range msgs {
		msg := uMsg.GetMessage()
		var pubMessage *PublishingMessage
		var err error
		pubMessage = uMsg.pubMsg
		if uMsg.pubMsg == nil {
			pubMessage, err = NewPublishingMessage(msg, p.cli.config.NameSpace, p.pSetting, txEnabled)
			if err != nil {
				return nil, err
			}
		}
		pubMessages[idx] = pubMessage
	}

	// check message Type
	messageType := pubMessages[0].messageType
	for _, pubMessage := range pubMessages {
		if pubMessage.messageType != messageType {
			return nil, fmt.Errorf("messages to send have different types, please check")
		}
	}

	var messageGroup *string
	// Message group must be same if message type is FIFO, or no need to proceed.
	if messageType == v2.MessageType_FIFO {
		messageGroup = pubMessages[0].msg.GetMessageGroup()
		for _, pubMessage := range pubMessages {
			if pubMessage.msg.GetMessageGroup() != messageGroup {
				return nil, fmt.Errorf("fifo messages to send have different message groups")
			}
		}
	}
	if _, ok := p.pSetting.topics.Load(topicName); !ok {
		p.pSetting.topics.Store(topicName, &v2.Resource{
			Name:              topicName,
			ResourceNamespace: p.cli.config.NameSpace,
		})
	}
	pubLoadBalancer, err := p.getPublishingTopicRouteResult(ctx, topicName)
	if err != nil {
		return nil, err
	}
	var candidates []*v2.MessageQueue
	if messageGroup == nil {
		candidates, err = p.takeMessageQueues(pubLoadBalancer)
	} else {
		candidates, err = pubLoadBalancer.TakeMessageQueueByMessageGroup(messageGroup)
	}
	if err != nil || len(candidates) == 0 {
		return nil, fmt.Errorf("no broker available to sendMessage")
	}
	return p.send1(ctx, topicName, messageType, candidates, pubMessages, 1)
}

func (p *defaultProducer) Send(ctx context.Context, msg *Message) ([]*SendReceipt, error) {
	if !p.isOn() {
		return nil, fmt.Errorf("producer is not running")
	}
	msgs := []*UnifiedMessage{{
		msg: msg,
	}}
	return p.send0(ctx, msgs, false)
}

func (p *defaultProducer) SendAsync(ctx context.Context, msg *Message, f func(context.Context, []*SendReceipt, error)) {
	if !p.isOn() {
		f(ctx, nil, fmt.Errorf("producer is not running"))
	}
	go func() {
		msgs := []*UnifiedMessage{{
			msg: msg,
		}}
		resp, err := p.send0(ctx, msgs, false)
		f(ctx, resp, err)
	}()
}

func (p *defaultProducer) SendWithTransaction(ctx context.Context, msg *Message, transaction Transaction) ([]*SendReceipt, error) {
	if !p.isOn() {
		return nil, fmt.Errorf("producer is not running")
	}
	t := transaction.(*transactionImpl)
	pubMessage, err := t.tryAddMessage(msg, p.cli.config.NameSpace)
	if err != nil {
		return nil, err
	}
	pubMsgs := []*UnifiedMessage{{
		pubMsg: pubMessage,
	}}
	resp, err := p.send0(ctx, pubMsgs, true)
	if err != nil {
		return nil, err
	}
	err2 := t.tryAddReceipt(pubMessage, resp[0])
	if err2 != nil {
		p.cli.log.Error(err2)
	}
	return resp, err
}

func (p *defaultProducer) GracefulStop() error {
	return p.cli.GracefulStop()
}

func (p *defaultProducer) BeginTransaction() Transaction {
	return NewTransactionImpl(p)
}

func (p *defaultProducer) endTransaction(ctx context.Context, endpoints *v2.Endpoints, messageCommon *MessageCommon,
	messageId string, transactionId string, resolution TransactionResolution) error {

	ctx = p.cli.Sign(ctx)
	request := &v2.EndTransactionRequest{
		Topic: &v2.Resource{
			Name:              messageCommon.topic,
			ResourceNamespace: p.cli.config.NameSpace,
		},
		MessageId:     messageId,
		TransactionId: transactionId,
	}
	switch resolution {
	case COMMIT:
		request.Resolution = v2.TransactionResolution_COMMIT
	case ROLLBACK:
		request.Resolution = v2.TransactionResolution_ROLLBACK
	default:
		request.Resolution = v2.TransactionResolution_TRANSACTION_RESOLUTION_UNSPECIFIED
	}
	requestTimeout := p.pSetting.requestTimeout
	messageHookPoints := MessageHookPoints_COMMIT_TRANSACTION
	if resolution == ROLLBACK {
		messageHookPoints = MessageHookPoints_ROLLBACK_TRANSACTION
	}
	messageCommons := []*MessageCommon{messageCommon}
	p.cli.doBefore(messageHookPoints, messageCommons)
	watchTime := time.Now()
	resp, err := p.cli.clientManager.EndTransaction(ctx, endpoints, request, requestTimeout)
	duration := time.Since(watchTime)
	messageHookPointsStatus := MessageHookPointsStatus_OK
	if err == nil && resp.GetStatus().GetCode() != v2.Code_OK {
		err = &ErrRpcStatus{
			Code:    int32(resp.Status.GetCode()),
			Message: resp.GetStatus().GetMessage(),
		}
	}
	if err != nil {
		messageHookPointsStatus = MessageHookPointsStatus_ERROR
	}
	p.cli.doAfter(messageHookPoints, messageCommons, duration, messageHookPointsStatus)
	return err
}

func (p *defaultProducer) onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) error {
	transactionId := command.GetTransactionId()
	messageId := command.GetMessage().GetSystemProperties().MessageId
	if p.checker == nil {
		return fmt.Errorf("no transaction checker registered, ignore it, messageId=%s, transactionId=%s, endpoints=%v", messageId, transactionId, endpoints)
	}
	messageView := fromProtobuf_MessageView0(command.Message)
	go func(mv *MessageView) {
		resolution := p.po.checker.Check(mv)
		err := p.endTransaction(context.TODO(), endpoints,
			mv.GetMessageCommon(), messageId, transactionId, resolution)
		if err != nil {
			p.cli.log.Errorf("exception raised while ending the transaction, messageId=%s, transactionId=%s, endpoints=%v, err=%w", messageId, transactionId, endpoints, err)
		}
	}(messageView)
	return nil
}

func (p *defaultProducer) onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) error {
	return nil
}

func (p *defaultProducer) SetRequestTimeout(timeout time.Duration) {
	p.cli.opts.timeout = timeout
	p.pSetting.requestTimeout = p.cli.opts.timeout
}

func (p *defaultProducer) IsEndpointUpdated() bool {
	return p.cli.ReceiveReconnect
}
