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
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/ticker"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
)

type LitePushConsumer interface {
	PushConsumer
	SubscribeLite(liteTopic string) error
	UnSubscribeLite(liteTopic string) error
}

var _ = LitePushConsumer(&defaultLitePushConsumer{})

type defaultLitePushConsumer struct {
	*defaultPushConsumer
	litePushConsumerSettings *litePushConsumerSettings
}

type LitePushConsumerConfig struct {
	bindTopic         string
	invisibleDuration time.Duration
}

func NewLitePushConsumerConfig(bindTopic string, invisibleDuration time.Duration) *LitePushConsumerConfig {
	return &LitePushConsumerConfig{
		bindTopic:         bindTopic,
		invisibleDuration: invisibleDuration,
	}
}

var NewLitePushConsumer = func(config *Config, liteConfig *LitePushConsumerConfig, opts ...PushConsumerOption) (LitePushConsumer, error) {
	if liteConfig.bindTopic == "" {
		return nil, errors.New("LitePushConsumerConfig.bindTopic is required")
	}
	filterExpressionMap := map[string]*FilterExpression{
		liteConfig.bindTopic: SUB_ALL,
	}
	if liteConfig == nil {
		return nil, errors.New("LitePushConsumerConfig is required")
	}
	opts = append(opts, WithPushSubscriptionExpressions(filterExpressionMap))
	if pushConsumer, err := newPushConsumer(config, opts...); err != nil {
		return nil, err
	} else {
		// force fifo
		pushConsumer.pcSettings.isFifo = true
		lpcSetting := newLitePushConsumerSettings(pushConsumer.pcSettings, liteConfig.bindTopic, liteConfig.invisibleDuration)
		pushConsumer.pcSettings.clientType = v2.ClientType_LITE_PUSH_CONSUMER
		lpcSetting.pushConsumerSettings.clientType = v2.ClientType_LITE_PUSH_CONSUMER
		pushConsumer.cli.settings = lpcSetting
		lpc := &defaultLitePushConsumer{
			defaultPushConsumer:      pushConsumer,
			litePushConsumerSettings: lpcSetting,
		}

		pushConsumer.pushConsumerExtension = lpc
		return lpc, nil
	}
}

func (lpc *defaultLitePushConsumer) Start() error {
	if err := lpc.startPushConsumer(); err != nil {
		return err
	}
	lpc.defaultPushConsumer.cli.notifyUnsubscribeLiteFunc = lpc.notifyUnsubscribeLite
	//todo
	lpc.syncAllLiteSubscription()
	ticker.Tick(lpc.syncAllLiteSubscription, time.Second*30, lpc.defaultPushConsumer.cli.done)
	return nil
}

func (lpc *defaultLitePushConsumer) startPushConsumer() error {
	return lpc.defaultPushConsumer.Start()
}

func (lpc *defaultLitePushConsumer) notifyUnsubscribeLite(command *v2.NotifyUnsubscribeLiteCommand) {
	liteTopic := command.LiteTopic
	sugarBaseLogger.Infof("LitePushConsumer notifyUnsubscribeLite liteTopic:%s", liteTopic)
	if liteTopic == "" {
		return
	}
	delete(lpc.litePushConsumerSettings.liteTopicSet, liteTopic)
}

func (lpc *defaultLitePushConsumer) SubscribeLite(topic string) error {
	if err := lpc.checkRunning(); err != nil {
		return err
	}
	if err := lpc.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_INCREMENTAL_ADD, []string{topic}); err != nil {
		sugarBaseLogger.Errorf("LitePushConsumer SubscribeLite topic:%s err:%v", topic, err)
		return err
	}
	lpc.litePushConsumerSettings.liteTopicSet[topic] = struct{}{}
	return nil
}

func (lpc *defaultLitePushConsumer) UnSubscribeLite(topic string) error {
	if err := lpc.checkRunning(); err != nil {
		return err
	}
	if err := lpc.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_INCREMENTAL_REMOVE, []string{topic}); err != nil {
		sugarBaseLogger.Errorf("LitePushConsumer UnSubscribeLite topic:%s err:%v", topic, err)
		return err
	}
	delete(lpc.litePushConsumerSettings.liteTopicSet, topic)
	return nil
}

func (lpc *defaultLitePushConsumer) checkRunning() error {
	if !lpc.defaultPushConsumer.isRunning() {
		sugarBaseLogger.Errorf("[bug] LitePushConsumer not running. clientId: %s", lpc.litePushConsumerSettings.clientId)
		return errors.New("consumer is not running")
	}
	return nil
}

func (lpc *defaultLitePushConsumer) syncAllLiteSubscription() {
	var liteTopicSet = make([]string, 0, len(lpc.litePushConsumerSettings.liteTopicSet))
	for k := range lpc.litePushConsumerSettings.liteTopicSet {
		liteTopicSet = append(liteTopicSet, k)
	}
	if len(liteTopicSet) == 0 {
		return
	}
	if err := lpc.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_ALL_ADD, liteTopicSet); err != nil {
		sugarBaseLogger.Errorf("LitePushConsumer syncAllLiteSubscription:%v,  err:%v", liteTopicSet, err)
	}
}

func (lpc *defaultLitePushConsumer) syncLiteSubscription(context context.Context, action v2.LiteSubscriptionAction, diff []string) error {
	endpoints := lpc.cli.accessPoint
	request := v2.SyncLiteSubscriptionRequest{
		Action: action,
		Topic: &v2.Resource{
			Name:              lpc.litePushConsumerSettings.bingTopic,
			ResourceNamespace: lpc.cli.config.NameSpace,
		},
		Group:        lpc.litePushConsumerSettings.groupName,
		LiteTopicSet: diff,
	}
	context = lpc.cli.Sign(context)
	if v, err := lpc.defaultPushConsumer.cli.clientManager.SyncLiteSubscription(context, endpoints, &request, lpc.pcSettings.requestTimeout); err != nil {
		return err
	} else {
		if v.GetStatus().GetCode() != v2.Code_OK {
			return &ErrRpcStatus{
				Code:    int32(v.Status.GetCode()),
				Message: v.GetStatus().GetMessage(),
			}
		}
		return nil
	}
}

var _ = PushConsumerExtension(&defaultLitePushConsumer{})

func (lpc *defaultLitePushConsumer) WrapReceiveMessageRequest(batchSize int, messageQueue *v2.MessageQueue, filterExpression *FilterExpression, longPollingTimeout time.Duration) *v2.ReceiveMessageRequest {
	attemptId := uuid.New().String()

	return &v2.ReceiveMessageRequest{
		Group: &v2.Resource{
			Name:              lpc.groupName,
			ResourceNamespace: lpc.cli.config.NameSpace,
		},
		MessageQueue:       messageQueue,
		LongPollingTimeout: durationpb.New(longPollingTimeout),
		BatchSize:          int32(batchSize),
		AutoRenew:          false,
		AttemptId:          &attemptId,
		InvisibleDuration:  durationpb.New(lpc.litePushConsumerSettings.invisibleDuration),
	}
}

func (lpc *defaultLitePushConsumer) WrapHeartbeatRequest() *v2.HeartbeatRequest {
	return &v2.HeartbeatRequest{
		Group:      lpc.pcSettings.groupName,
		ClientType: v2.ClientType_LITE_PUSH_CONSUMER,
	}
}
