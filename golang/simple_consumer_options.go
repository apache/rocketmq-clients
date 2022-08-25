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

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

type FilterExpressionType int32

const (
	SQL92 FilterExpressionType = iota
	TAG
	UNSPECIFIED
)

type FilterExpression struct {
	expression     string
	expressionType FilterExpressionType
}

var SUB_ALL = NewFilterExpression("*")

var NewFilterExpression = func(expression string) *FilterExpression {
	return &FilterExpression{
		expression:     expression,
		expressionType: TAG,
	}
}

var NewFilterExpressionWithType = func(expression string, expressionType FilterExpressionType) *FilterExpression {
	return &FilterExpression{
		expression:     expression,
		expressionType: expressionType,
	}
}

type simpleConsumerOptions struct {
	subscriptionExpressions map[string]*FilterExpression
	awaitDuration           time.Duration
	clientFunc              NewClientFunc
}

var defaultSimpleConsumerOptions = simpleConsumerOptions{
	clientFunc: NewClient,
}

// A ConsumerOption sets options such as tag, etc.
type SimpleConsumerOption interface {
	apply(*simpleConsumerOptions)
}

// funcOption wraps a function that modifies options into an implementation of
// the Option interface.
type funcSimpleConsumerOption struct {
	f func(*simpleConsumerOptions)
}

func (fo *funcSimpleConsumerOption) apply(do *simpleConsumerOptions) {
	fo.f(do)
}

func newFuncSimpleConsumerOption(f func(*simpleConsumerOptions)) *funcSimpleConsumerOption {
	return &funcSimpleConsumerOption{
		f: f,
	}
}

// WithTag returns a consumerOption that sets tag for consumer.
// Note: Default it uses *.
func WithSubscriptionExpressions(subscriptionExpressions map[string]*FilterExpression) SimpleConsumerOption {
	return newFuncSimpleConsumerOption(func(o *simpleConsumerOptions) {
		o.subscriptionExpressions = subscriptionExpressions
	})
}

func WithAwaitDuration(awaitDuration time.Duration) SimpleConsumerOption {
	return newFuncSimpleConsumerOption(func(o *simpleConsumerOptions) {
		o.awaitDuration = awaitDuration
	})
}

var _ = ClientSettings(&simpleConsumerSettings{})

type simpleConsumerSettings struct {
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
func (sc *simpleConsumerSettings) GetAccessPoint() *v2.Endpoints {
	return sc.endpoints
}

// GetClientID implements ClientSettings
func (sc *simpleConsumerSettings) GetClientID() string {
	return sc.clientId
}

// GetClientType implements ClientSettings
func (sc *simpleConsumerSettings) GetClientType() v2.ClientType {
	return sc.clientType
}

// GetRequestTimeout implements ClientSettings
func (sc *simpleConsumerSettings) GetRequestTimeout() time.Duration {
	return sc.requestTimeout
}

// GetRetryPolicy implements ClientSettings
func (sc *simpleConsumerSettings) GetRetryPolicy() *v2.RetryPolicy {
	return sc.retryPolicy
}

// applySettingsCommand implements ClientSettings
func (sc *simpleConsumerSettings) applySettingsCommand(settings *v2.Settings) error {
	pubSub := settings.GetPubSub()
	if pubSub == nil {
		return fmt.Errorf("onSettingsCommand err = pubSub is nil")
	}
	_, ok := pubSub.(*v2.Settings_Subscription)
	if !ok {
		return fmt.Errorf("[bug] Issued settings not match with the client type, clientId=%v, clientType=%v", sc.GetClientID(), sc.GetClientType())
	}
	return nil
}

// toProtobuf implements ClientSettings
func (sc *simpleConsumerSettings) toProtobuf() *v2.Settings {
	subscriptions := make([]*v2.SubscriptionEntry, 0)
	for k, v := range sc.subscriptionExpressions {
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
			sugarBaseLogger.Warnf("[bug] Unrecognized filter type for simple consumer, type=%v, client_id=%v", v.expressionType, sc.clientId)
		}
		subscriptions = append(subscriptions, &v2.SubscriptionEntry{
			Topic:      topic,
			Expression: filterExpression,
		})
	}
	subSetting := &v2.Settings_Subscription{
		Subscription: &v2.Subscription{
			Group:              sc.groupName,
			Subscriptions:      subscriptions,
			LongPollingTimeout: durationpb.New(sc.longPollingTimeout),
		},
	}
	settings := &v2.Settings{
		ClientType:     &sc.clientType,
		AccessPoint:    sc.GetAccessPoint(),
		RequestTimeout: durationpb.New(sc.requestTimeout),
		PubSub:         subSetting,
		UserAgent:      globalUserAgent.toProtoBuf(),
	}
	return settings
}
