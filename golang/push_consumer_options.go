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
	"sync"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ConsumerResult int8

const (
	/**
	 * Consume message successfully.
	 */
	SUCCESS ConsumerResult = 0
	/**
	 * Failed to consume message.
	 */
	FAILURE ConsumerResult = 1
)

type MessageListener interface {
	consume(*MessageView) ConsumerResult
}

type FuncMessageListener struct {
	Consume func(*MessageView) ConsumerResult
}

// consume implements MessageListener
func (l *FuncMessageListener) consume(msg *MessageView) ConsumerResult {
	return l.Consume(msg)
}

var _ = MessageListener(&FuncMessageListener{})

type pushConsumerOptions struct {
	subscriptionExpressions    *sync.Map
	awaitDuration              time.Duration
	maxCacheMessageCount       int32
	maxCacheMessageSizeInBytes int64
	consumptionThreadCount     int32
	messageListener            MessageListener
	clientFunc                 NewClientFunc
}

var defaultPushConsumerOptions = pushConsumerOptions{
	clientFunc:                 NewClient,
	awaitDuration:              0,
	maxCacheMessageCount:       1024,
	maxCacheMessageSizeInBytes: 64 * 1024 * 1024,
	consumptionThreadCount:     20,
}

// A ConsumerOption sets options such as tag, etc.
type PushConsumerOption interface {
	apply(*pushConsumerOptions)
}

// funcOption wraps a function that modifies options into an implementation of
// the Option interface.
type funcPushConsumerOption struct {
	f func(*pushConsumerOptions)
}

func (option *funcPushConsumerOption) apply(do *pushConsumerOptions) {
	option.f(do)
}

func newFuncPushConsumerOption(f func(*pushConsumerOptions)) *funcPushConsumerOption {
	return &funcPushConsumerOption{
		f: f,
	}
}

// WithTag returns a consumerOption that sets tag for consumer.
// Note: Default it uses *.
func WithPushSubscriptionExpressions(subscriptionExpressions map[string]*FilterExpression) PushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		if o.subscriptionExpressions == nil {
			o.subscriptionExpressions = &sync.Map{}
		}
		for k, v := range subscriptionExpressions {
			o.subscriptionExpressions.Store(k, v)
		}
	})
}

func WithPushAwaitDuration(awaitDuration time.Duration) PushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		o.awaitDuration = awaitDuration
	})
}

func WithPushMaxCacheMessageSizeInBytes(maxCacheMessageSizeInBytes int64) PushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		o.maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes
	})
}

func WithPushMaxCacheMessageCount(maxCacheMessageCount int32) PushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		o.maxCacheMessageCount = maxCacheMessageCount
	})
}

func WithPushConsumptionThreadCount(consumptionThreadCount int32) PushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		o.consumptionThreadCount = consumptionThreadCount
	})
}

func WithPushMessageListener(messageListener MessageListener) PushConsumerOption {
	return newFuncPushConsumerOption(func(o *pushConsumerOptions) {
		o.messageListener = messageListener
	})
}

var _ = ClientSettings(&pushConsumerSettings{})

type pushConsumerSettings struct {
	clientId       string
	endpoints      *v2.Endpoints
	clientType     v2.ClientType
	retryPolicy    *v2.RetryPolicy
	requestTimeout time.Duration

	isFifo           bool
	receiveBatchSize int32

	groupName               *v2.Resource
	longPollingTimeout      time.Duration
	subscriptionExpressions *sync.Map
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
	v, ok := pubSub.(*v2.Settings_Subscription)
	if !ok {
		return fmt.Errorf("[bug] Issued settings not match with the client type, clientId=%v, clientType=%v", pc.GetClientID(), pc.GetClientType())
	}
	pc.isFifo = v.Subscription.GetFifo()
	pc.receiveBatchSize = v.Subscription.GetReceiveBatchSize()
	pc.longPollingTimeout = v.Subscription.GetLongPollingTimeout().AsDuration()

	backoffPolicy := settings.GetBackoffPolicy()
	if backoffPolicy != nil {
		pc.retryPolicy.MaxAttempts = backoffPolicy.MaxAttempts
		if exponentialBackoff := backoffPolicy.GetExponentialBackoff(); exponentialBackoff != nil {
			pc.retryPolicy.Strategy = &v2.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &v2.ExponentialBackoff{
					Max:        exponentialBackoff.GetMax(),
					Initial:    exponentialBackoff.GetInitial(),
					Multiplier: exponentialBackoff.GetMultiplier(),
				},
			}
		} else if customizedBackoff := backoffPolicy.GetCustomizedBackoff(); customizedBackoff != nil {
			pc.retryPolicy.Strategy = &v2.RetryPolicy_CustomizedBackoff{
				CustomizedBackoff: &v2.CustomizedBackoff{
					Next: customizedBackoff.GetNext(),
				},
			}
		} else {
			return fmt.Errorf("onSettingsCommand err = unrecognized backoff policy strategy")
		}
	} else {
		return fmt.Errorf("onSettingsCommand err = backoffPolicy is nil")
	}
	return nil
}

// toProtobuf implements ClientSettings
func (pc *pushConsumerSettings) toProtobuf() *v2.Settings {
	subscriptions := make([]*v2.SubscriptionEntry, 0)
	pc.subscriptionExpressions.Range(func(key, value interface{}) bool {
		k := key.(string)
		v := value.(*FilterExpression)
		topic := &v2.Resource{
			Name:              k,
			ResourceNamespace: pc.groupName.GetResourceNamespace(),
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
			sugarBaseLogger.Warnf("[bug] Unrecognized filter type for simple consumer, type=%v, client_id=%v", v.expressionType, pc.clientId)
		}
		subscriptions = append(subscriptions, &v2.SubscriptionEntry{
			Topic:      topic,
			Expression: filterExpression,
		})
		return true
	})
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
