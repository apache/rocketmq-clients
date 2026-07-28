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

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// LiteSimpleConsumer is similar to SimpleConsumer, but for lite topic.
type LiteSimpleConsumer interface {
	SimpleConsumer
	SubscribeLite(liteTopic string, offsetOption ...OffsetOption) error
	UnSubscribeLite(liteTopic string) error
	GetLiteTopicSet() []string
}

var _ = LiteSimpleConsumer(&defaultLiteSimpleConsumer{})

type defaultLiteSimpleConsumer struct {
	*defaultSimpleConsumer
	liteSimpleConsumerSettings *liteSimpleConsumerSettings
	liteSubscriptionManager    *liteSubscriptionManager
}

type LiteSimpleConsumerConfig struct {
	bindTopic string
}

func NewLiteSimpleConsumerConfig(bindTopic string) *LiteSimpleConsumerConfig {
	return &LiteSimpleConsumerConfig{
		bindTopic: bindTopic,
	}
}

var NewLiteSimpleConsumer = func(config *Config, liteConfig *LiteSimpleConsumerConfig, opts ...SimpleConsumerOption) (LiteSimpleConsumer, error) {
	if liteConfig == nil {
		return nil, errors.New("LiteSimpleConsumerConfig is required")
	}
	if liteConfig.bindTopic == "" {
		return nil, errors.New("LiteSimpleConsumerConfig.bindTopic is required")
	}
	// Default subscription: (bindTopic, *) for code reuse.
	filterExpressionMap := map[string]*FilterExpression{
		liteConfig.bindTopic: SUB_ALL,
	}
	opts = append(opts, WithSimpleSubscriptionExpressions(filterExpressionMap))
	if simpleConsumer, err := newSimpleConsumer(config, opts...); err != nil {
		return nil, err
	} else {
		lscSettings := newLiteSimpleConsumerSettings(simpleConsumer.scSettings, liteConfig.bindTopic)
		simpleConsumer.scSettings.clientType = v2.ClientType_LITE_SIMPLE_CONSUMER
		simpleConsumer.cli.settings = lscSettings
		lsc := &defaultLiteSimpleConsumer{
			defaultSimpleConsumer:      simpleConsumer,
			liteSimpleConsumerSettings: lscSettings,
		}
		lsc.liteSubscriptionManager = newLiteSubscriptionManager(lsc,
			liteConfig.bindTopic, lscSettings.groupName, simpleConsumer.cli.config.NameSpace)
		lscSettings.liteSubscriptionManager = lsc.liteSubscriptionManager
		return lsc, nil
	}
}

func (lsc *defaultLiteSimpleConsumer) Start() error {
	if err := lsc.defaultSimpleConsumer.Start(); err != nil {
		return err
	}
	lsc.defaultSimpleConsumer.cli.notifyUnsubscribeLiteFunc = lsc.liteSubscriptionManager.onNotifyUnsubscribeLiteCommand
	lsc.liteSubscriptionManager.startUp()
	return nil
}

func (lsc *defaultLiteSimpleConsumer) SubscribeLite(liteTopic string, offsetOption ...OffsetOption) error {
	return lsc.liteSubscriptionManager.subscribeLite(liteTopic, offsetOption...)
}

func (lsc *defaultLiteSimpleConsumer) UnSubscribeLite(liteTopic string) error {
	return lsc.liteSubscriptionManager.unsubscribeLite(liteTopic)
}

func (lsc *defaultLiteSimpleConsumer) GetLiteTopicSet() []string {
	return lsc.liteSubscriptionManager.getLiteTopicSet()
}
