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
	"fmt"
	"log"
	"sync"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"github.com/apache/rocketmq-clients/golang/utils"
)

type Producer interface {
	Send(context.Context, *Message) ([]*SendReceipt, error)
	SendAsync(context.Context, *Message, func(context.Context, []*SendReceipt, error))
	GracefulStop() error
}

type producer struct {
	po  producerOptions
	cli *defaultClient

	isolated                       sync.Map
	publishingRouteDataResultCache sync.Map
}

var _ = Producer(&producer{})

func (p *producer) takeMessageQueues(plb PublishingLoadBalancer) ([]*v2.MessageQueue, error) {
	return plb.TakeMessageQueues(p.isolated, p.getRetryMaxAttempts())
}

func (p *producer) getPublishingTopicRouteResult(ctx context.Context, topic string) (PublishingLoadBalancer, error) {
	item, ok := p.publishingRouteDataResultCache.Load(topic)
	if ok {
		if ret, ok := item.(PublishingLoadBalancer); ok {
			return ret, nil
		}
	}
	route, err := p.cli.GetMessageQueues(ctx, topic)
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

func (p *producer) wrapSendMessageRequest(pMsgs []*PublishingMessage) (*v2.SendMessageRequest, error) {
	smr := &v2.SendMessageRequest{
		Messages: []*v2.Message{},
	}
	for _, pMsg := range pMsgs {
		msgV2, err := pMsg.toProtobuf()
		if err != nil {
			return nil, fmt.Errorf("wrapSendMessageRequest faild, {%v}", err)
		}
		smr.Messages = append(smr.Messages, msgV2)
	}
	return smr, nil
}

func NewProducer(config *Config, opts ...ProducerOption) (Producer, error) {
	po := &defaultProducerOptions
	for _, opt := range opts {
		opt.apply(po)
	}
	cli, err := po.clientFunc(config)
	if err != nil {
		return nil, err
	}

	return &producer{
		po:  *po,
		cli: cli.(*defaultClient),
	}, nil
}
func (p *producer) getRetryMaxAttempts() int {
	return 3
}
func (p *producer) getNextAttemptDelay(nextAttempt int) time.Duration {
	return time.Second
}

// TODO refer to java sdk.
func (p *producer) send1(ctx context.Context, topic string, messageType v2.MessageType,
	candidates []*v2.MessageQueue, pubMessages []*PublishingMessage, attempt int) ([]*SendReceipt, error) {

	ctx = p.cli.Sign(ctx)

	idx := utils.Mod(int32(attempt)-1, len(candidates))
	selectMessageQueue := candidates[idx]

	// TODO Determine whether the messageType matches.
	// producerSettings.isValidateMessageType() && !messageQueue.matchMessageType(messageType)

	endpoints := selectMessageQueue.GetBroker().GetEndpoints()
	sendReq, err := p.wrapSendMessageRequest(pubMessages)
	if err != nil {
		return nil, err
	}
	resp, err := p.cli.clientManager.SendMessage(ctx, endpoints, sendReq, time.Second*5)
	// processSendResponse
	tooManyRequests := false
	if err == nil && resp.GetStatus().GetCode() != v2.Code_OK {
		tooManyRequests = resp.GetStatus().GetCode() == v2.Code_TOO_MANY_REQUESTS
		err = errors.New(resp.String())
	}
	maxAttempts := p.getRetryMaxAttempts()
	if err != nil {
		// retry
		for _, address := range endpoints.GetAddresses() {
			p.isolated.Store(address.String(), true)
		}
		if attempt >= maxAttempts {
			log.Printf("Failed to send message(s) finally, run out of attempt times, topic=%s, maxAttempts=%d, attempt=%d, endpoints=%s, clientId=%s",
				topic, maxAttempts, attempt, endpoints.String(), p.cli.clientID)
			return nil, err
		}
		// No need more attempts for transactional message.
		if messageType == v2.MessageType_TRANSACTION {
			log.Printf("Failed to send transactional message finally, topic=%s, maxAttempts=%d, attempt=%d, endpoints=%s, clientId=%s",
				topic, 1, attempt, endpoints.String(), p.cli.clientID)
			return nil, err
		}
		// Try to do more attempts.
		nextAttempt := attempt + 1
		// Retry immediately if the request is not throttled.
		if tooManyRequests {
			time.Sleep(p.getNextAttemptDelay(nextAttempt))
		}
		return p.send1(ctx, topic, messageType, candidates, pubMessages, nextAttempt)
	}

	var res []*SendReceipt
	for i := 0; i < len(resp.GetEntries()); i++ {
		res = append(res, &SendReceipt{
			MessageID: resp.GetEntries()[i].GetMessageId(),
		})
	}
	if attempt > 1 {
		log.Printf("Resend message successfully, topic=%s, maxAttempts=%d, attempt=%d, endpoints=%s, clientId=%s",
			topic, maxAttempts, attempt, endpoints.String(), p.cli.clientID)
	}
	return res, nil
	// sendRequest := b.getSendMessageRequest(msg)

	// b, err := p.ns.GetBroker(ctx, msg.Topic)
	// if err != nil {
	// 	return nil, err
	// }
	// return b.Send(ctx, msg)

	// return nil, nil
}

// TODO refer to java sdk.
func (p *producer) send0(ctx context.Context, msgs []*Message, txEnabled bool) ([]*SendReceipt, error) {
	// check topic Name
	topicName := msgs[0].Topic
	for _, msg := range msgs {
		if msg.Topic != topicName {
			return nil, fmt.Errorf("Messages to send have different topics")
		}
	}

	pubMessages := make([]*PublishingMessage, len(msgs))
	for idx, msg := range msgs {
		pubMessage, err := NewPublishingMessage(msg, txEnabled)
		if err != nil {
			return nil, err
		}
		pubMessages[idx] = pubMessage
	}

	// check topic Name
	messageType := pubMessages[0].messageType
	for _, pubMessage := range pubMessages {
		if pubMessage.messageType != messageType {
			return nil, fmt.Errorf("Messages to send have different types, please check")
		}
	}

	var messageGroup string
	// Message group must be same if message type is FIFO, or no need to proceed.
	if messageType == v2.MessageType_FIFO {
		messageGroup = pubMessages[0].msg.GetMessageGroup()
		for _, pubMessage := range pubMessages {
			if pubMessage.msg.GetMessageGroup() != messageGroup {
				return nil, fmt.Errorf("FIFO messages to send have different message groups")
			}
		}
	}

	pubLoadBalancer, err := p.getPublishingTopicRouteResult(ctx, topicName)
	if err != nil {
		return nil, err
	}
	var candidates []*v2.MessageQueue
	if len(messageGroup) == 0 {
		candidates, err = p.takeMessageQueues(pubLoadBalancer)
		for _, v := range candidates {
			str := v.Broker.Endpoints.String()
			fmt.Println(str)
		}
	} else {
		candidates, err = pubLoadBalancer.TakeMessageQueueByMessageGroup(messageGroup)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no broker available to sendMessage")
	}
	return p.send1(ctx, topicName, messageType, candidates, pubMessages, 1)

	// broker, err := p.ns.GetBroker(ctx, msgs[0].Topic)
	// if err != nil {
	// 	return nil, err
	// }
	// return broker.Send(ctx, msgs[0])
}

func (p *producer) Send(ctx context.Context, msg *Message) ([]*SendReceipt, error) {
	msgs := []*Message{msg}
	return p.send0(ctx, msgs, false)
}

func (p *producer) SendAsync(ctx context.Context, msg *Message, f func(context.Context, []*SendReceipt, error)) {
	go func() {
		msgs := []*Message{msg}
		resp, err := p.send0(ctx, msgs, false)
		f(ctx, resp, err)
	}()
}

func (p *producer) GracefulStop() error {
	return p.cli.GracefulStop()
}
