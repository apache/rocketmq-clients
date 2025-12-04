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
	"fmt"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

var _ = ClientSettings(&litePushConsumerSettings{})

type litePushConsumerSettings struct {
	*pushConsumerSettings
	bingTopic             string
	liteTopicSet          map[string]struct{}
	liteSubscriptionQuota int32 // default 1200
	maxLiteTopicSize      int32
	invisibleDuration     time.Duration
}

func newLitePushConsumerSettings(settings *pushConsumerSettings, bindTopic string, invisibleDuration time.Duration) *litePushConsumerSettings {
	return &litePushConsumerSettings{
		pushConsumerSettings: settings,
		bingTopic:            bindTopic,
		liteTopicSet:         map[string]struct{}{},
		invisibleDuration:    invisibleDuration,
		// default value
		liteSubscriptionQuota: 1200,
		maxLiteTopicSize:      64,
	}
}

// GetAccessPoint implements ClientSettings
func (lpc *litePushConsumerSettings) GetAccessPoint() *v2.Endpoints {
	return lpc.pushConsumerSettings.endpoints
}

// GetClientID implements ClientSettings
func (lpc *litePushConsumerSettings) GetClientID() string {
	return lpc.pushConsumerSettings.clientId
}

// GetClientType implements ClientSettings
func (lpc *litePushConsumerSettings) GetClientType() v2.ClientType {
	return lpc.pushConsumerSettings.clientType
}

// GetRequestTimeout implements ClientSettings
func (lpc *litePushConsumerSettings) GetRequestTimeout() time.Duration {
	return lpc.pushConsumerSettings.requestTimeout
}

// GetRetryPolicy implements ClientSettings
func (lpc *litePushConsumerSettings) GetRetryPolicy() *v2.RetryPolicy {
	return lpc.pushConsumerSettings.retryPolicy
}

// applySettingsCommand implements ClientSettings
func (lpc *litePushConsumerSettings) applySettingsCommand(settings *v2.Settings) error {
	if lpc.pushConsumerSettings.applySettingsCommand(settings) != nil {
		sugarBaseLogger.Warnf("litePushConsumerSettings applySettingsCommand failed")
		return fmt.Errorf("litePushConsumerSettings applySettingsCommand failed")
	}
	// force fifo to true
	lpc.pushConsumerSettings.isFifo = true
	var subscription = settings.GetSubscription()
	if subscription == nil {
		sugarBaseLogger.Warnf("onSettingsCommand err = subscription is nil")
		return fmt.Errorf("onSettingsCommand err = subscription is nil")
	}
	if subscription.LiteSubscriptionQuota != nil {
		lpc.liteSubscriptionQuota = *subscription.LiteSubscriptionQuota
	}
	if subscription.MaxLiteTopicSize != nil {
		lpc.maxLiteTopicSize = *subscription.MaxLiteTopicSize
	}
	return nil
}

// toProtobuf implements ClientSettings
func (lpc *litePushConsumerSettings) toProtobuf() *v2.Settings {
	subscriptions := make([]*v2.SubscriptionEntry, 0)
	lpc.subscriptionExpressions.Range(func(key, value interface{}) bool {
		k := key.(string)
		v := value.(*FilterExpression)
		topic := &v2.Resource{
			Name:              k,
			ResourceNamespace: lpc.groupName.GetResourceNamespace(),
		}
		filterExpression := &v2.FilterExpression{
			Expression: v.expression,
		}
		switch v.expressionType {
		case SQL92:
			filterExpression.Type = v2.FilterType_SQL
		case TAG:
			filterExpression.Type = v2.FilterType_TAG
		default:
			filterExpression.Type = v2.FilterType_FILTER_TYPE_UNSPECIFIED
			sugarBaseLogger.Warnf("[bug] Unrecognized filter type for simple consumer, type=%v, client_id=%v", v.expressionType, lpc.clientId)
		}
		subscriptions = append(subscriptions, &v2.SubscriptionEntry{
			Topic:      topic,
			Expression: filterExpression,
		})
		return true
	})
	subSetting := &v2.Settings_Subscription{
		Subscription: &v2.Subscription{
			Group:                 lpc.groupName,
			Subscriptions:         subscriptions,
			LongPollingTimeout:    durationpb.New(lpc.longPollingTimeout),
			LiteSubscriptionQuota: &lpc.liteSubscriptionQuota,
			MaxLiteTopicSize:      &lpc.maxLiteTopicSize,
		},
	}
	settings := &v2.Settings{
		ClientType:     &lpc.clientType,
		AccessPoint:    lpc.GetAccessPoint(),
		RequestTimeout: durationpb.New(lpc.requestTimeout),
		PubSub:         subSetting,
		UserAgent:      globalUserAgent.toProtoBuf(),
	}
	return settings
}
