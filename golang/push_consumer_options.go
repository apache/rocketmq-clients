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

type pushConsumerOptions struct {
	consumerOptions      *consumerOptions
	messageViewCacheSize int
	invisibleDuration    time.Duration
	consumeFunctions     map[string]*PushConsumerCallback
}

var defaultPushConsumerOptions = pushConsumerOptions{
	consumerOptions: &defaultConsumerOptions,
}

// A PushConsumerOption sets options such as tag, etc.
type PushConsumerOption interface {
	ConsumerOption
	apply0(*pushConsumerOptions)
}

// FuncPushConsumerOption wraps a function that modifies options into an implementation of
// the Option interface.
type FuncPushConsumerOption struct {
	*FuncConsumerOption
	f1 func(*pushConsumerOptions)
}

func (funcPushConsumerOption *FuncPushConsumerOption) apply(do *consumerOptions) {
	funcPushConsumerOption.f(do)
}

func (funcPushConsumerOption *FuncPushConsumerOption) apply0(do *pushConsumerOptions) {
	funcPushConsumerOption.f1(do)
}

func newFuncPushConsumerOption(f1 func(*pushConsumerOptions)) *FuncPushConsumerOption {
	return &FuncPushConsumerOption{
		f1: f1,
	}
}

func WithMessageViewCacheSize(messageViewCacheSize int) *FuncPushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		o.messageViewCacheSize = messageViewCacheSize
	})
}

func WithInvisibleDuration(invisibleDuration time.Duration) *FuncPushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		o.invisibleDuration = invisibleDuration
	})
}

func WithConsumeFunctions(consumeFunctions map[string]*PushConsumerCallback) *FuncPushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		o.consumeFunctions = consumeFunctions
	})

}

var _ = ClientSettings(&pushConsumerSettings{})

type pushConsumerSettings struct {
	clientId       string
	endpoints      *v2.Endpoints
	clientType     v2.ClientType
	retryPolicy    *v2.RetryPolicy
	requestTimeout time.Duration

	groupName               *v2.Resource
	longPollingTimeout      time.Duration
	subscriptionExpressions map[string]*FilterExpression
}

// GetAccessPoint implements ClientSettings
func (pc *pushConsumerSettings) GetAccessPoint() *v2.Endpoints {
	return pc.endpoints
}

// GetClientID implements ClientSettings
func (pc *pushConsumerSettings) GetClientID() string {
	return pc.clientId
}

// GetClientType implements ClientSettings
func (pc *pushConsumerSettings) GetClientType() v2.ClientType {
	return pc.clientType
}

// GetRequestTimeout implements ClientSettings
func (pc *pushConsumerSettings) GetRequestTimeout() time.Duration {
	return pc.requestTimeout
}

// GetRetryPolicy implements ClientSettings
func (pc *pushConsumerSettings) GetRetryPolicy() *v2.RetryPolicy {
	return pc.retryPolicy
}

// applySettingsCommand implements ClientSettings
func (pc *pushConsumerSettings) applySettingsCommand(settings *v2.Settings) error {
	pubSub := settings.GetPubSub()
	if pubSub == nil {
		return fmt.Errorf("onSettingsCommand err = pubSub is nil")
	}
	_, ok := pubSub.(*v2.Settings_Subscription)
	if !ok {
		return fmt.Errorf("[bug] Issued settings not match with the client type, clientId=%v, clientType=%v", pc.GetClientID(), pc.GetClientType())
	}
	return nil
}

// toProtobuf implements ClientSettings
func (pc *pushConsumerSettings) toProtobuf() *v2.Settings {
	subscriptions := make([]*v2.SubscriptionEntry, 0)
	for k, v := range pc.subscriptionExpressions {
		topic := &v2.Resource{
			Name: k,
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
			sugarBaseLogger.Warnf("[bug] Unrecognized filter type for push consumer, type=%v, client_id=%v", v.expressionType, pc.clientId)
		}
		subscriptions = append(subscriptions, &v2.SubscriptionEntry{
			Topic:      topic,
			Expression: filterExpression,
		})
	}
	subSetting := &v2.Settings_Subscription{
		Subscription: &v2.Subscription{
			Group:              pc.groupName,
			Subscriptions:      subscriptions,
			LongPollingTimeout: durationpb.New(pc.longPollingTimeout),
		},
	}
	settings := &v2.Settings{
		ClientType:     &pc.clientType,
		AccessPoint:    pc.GetAccessPoint(),
		RequestTimeout: durationpb.New(pc.requestTimeout),
		PubSub:         subSetting,
		UserAgent:      globalUserAgent.toProtoBuf(),
	}
	return settings
}
