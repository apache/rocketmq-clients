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
	"errors"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

type LitePushConsumer interface {
	PushConsumer
	SubscribeLite(liteTopic string, offsetOption ...OffsetOption) error
	UnSubscribeLite(liteTopic string) error
	GetLiteTopicSet() []string
}

var _ = LitePushConsumer(&defaultLitePushConsumer{})

type defaultLitePushConsumer struct {
	*defaultPushConsumer
	litePushConsumerSettings *litePushConsumerSettings
	liteSubscriptionManager  *liteSubscriptionManager
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
	if liteConfig == nil {
		return nil, errors.New("LitePushConsumerConfig is required")
	}
	if liteConfig.bindTopic == "" {
		return nil, errors.New("LitePushConsumerConfig.bindTopic is required")
	}
	filterExpressionMap := map[string]*FilterExpression{
		liteConfig.bindTopic: SUB_ALL,
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
		lpc.liteSubscriptionManager = newLiteSubscriptionManager(lpc,
			liteConfig.bindTopic, lpcSetting.groupName, pushConsumer.cli.config.NameSpace)
		lpcSetting.liteSubscriptionManager = lpc.liteSubscriptionManager
		pushConsumer.pushConsumerExtension = lpc
		return lpc, nil
	}
}

func (lpc *defaultLitePushConsumer) Start() error {
	if err := lpc.defaultPushConsumer.Start(); err != nil {
		return err
	}
	lpc.defaultPushConsumer.cli.notifyUnsubscribeLiteFunc = lpc.liteSubscriptionManager.onNotifyUnsubscribeLiteCommand
	lpc.liteSubscriptionManager.startUp()
	return nil
}

func (lpc *defaultLitePushConsumer) SubscribeLite(liteTopic string, offsetOption ...OffsetOption) error {
	return lpc.liteSubscriptionManager.subscribeLite(liteTopic, offsetOption...)
}

func (lpc *defaultLitePushConsumer) UnSubscribeLite(liteTopic string) error {
	return lpc.liteSubscriptionManager.unsubscribeLite(liteTopic)
}

func (lpc *defaultLitePushConsumer) GetLiteTopicSet() []string {
	return lpc.liteSubscriptionManager.getLiteTopicSet()
}

var _ = PushConsumerExtension(&defaultLitePushConsumer{})

func (lpc *defaultLitePushConsumer) WrapHeartbeatRequest() *v2.HeartbeatRequest {
	return &v2.HeartbeatRequest{
		Group:      lpc.pcSettings.groupName,
		ClientType: v2.ClientType_LITE_PUSH_CONSUMER,
	}
}
