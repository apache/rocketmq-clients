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

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

type producerOptions struct {
	clientFunc  NewClientFunc
	maxAttempts int32
	topics      []string
	checker     *TransactionChecker
}

var defaultProducerOptions = producerOptions{
	clientFunc:  NewClient,
	maxAttempts: 3,
}

// A ProducerOption sets options such as tls.Config, etc.
type ProducerOption interface {
	apply(*producerOptions)
}

// funcProducerOption wraps a function that modifies options into an implementation of
// the ConnOption interface.
type funcProducerOption struct {
	f func(options *producerOptions)
}

func (fpo *funcProducerOption) apply(po *producerOptions) {
	fpo.f(po)
}

func newFuncProducerOption(f func(options *producerOptions)) *funcProducerOption {
	return &funcProducerOption{
		f: f,
	}
}

// WithClientFunc returns a ProducerOption that sets ClientFunc for producer.
// Default is nameserver.New.
func WithClientFunc(f NewClientFunc) ProducerOption {
	return newFuncProducerOption(func(o *producerOptions) {
		o.clientFunc = f
	})
}

func WithMaxAttempts(m int32) ProducerOption {
	return newFuncProducerOption(func(o *producerOptions) {
		o.maxAttempts = m
	})
}

func WithTopics(t ...string) ProducerOption {
	return newFuncProducerOption(func(o *producerOptions) {
		o.topics = t
	})
}

func WithTransactionChecker(checker *TransactionChecker) ProducerOption {
	return newFuncProducerOption(func(o *producerOptions) {
		o.checker = checker
	})
}

var _ = ClientSettings(&producerSettings{})

type producerSettings struct {
	clientId            string
	topics              sync.Map
	endpoints           *v2.Endpoints
	clientType          v2.ClientType
	retryPolicy         *v2.RetryPolicy
	requestTimeout      time.Duration
	validateMessageType bool
	maxBodySizeBytes    int
}

func (ps *producerSettings) GetClientID() string {
	return ps.clientId
}
func (ps *producerSettings) GetAccessPoint() *v2.Endpoints {
	return ps.endpoints
}
func (ps *producerSettings) GetClientType() v2.ClientType {
	return ps.clientType
}
func (ps *producerSettings) GetRetryPolicy() *v2.RetryPolicy {
	return ps.retryPolicy
}
func (ps *producerSettings) GetRequestTimeout() time.Duration {
	return ps.requestTimeout
}
func (ps *producerSettings) IsValidateMessageType() bool {
	return ps.validateMessageType
}

func (ps *producerSettings) toProtobuf() *v2.Settings {
	topicList := make([]*v2.Resource, 0)
	ps.topics.Range(func(_, value interface{}) bool {
		topicList = append(topicList, value.(*v2.Resource))
		return true
	})
	pubSetting := &v2.Settings_Publishing{
		Publishing: &v2.Publishing{
			Topics: topicList,
		},
	}
	settings := &v2.Settings{
		ClientType:     &ps.clientType,
		AccessPoint:    ps.GetAccessPoint(),
		RequestTimeout: durationpb.New(ps.requestTimeout),
		PubSub:         pubSetting,
		BackoffPolicy:  ps.GetRetryPolicy(),
		UserAgent:      globalUserAgent.toProtoBuf(),
	}
	return settings
}

func (ps *producerSettings) applySettingsCommand(settings *v2.Settings) error {
	pubSub := settings.GetPubSub()
	if pubSub == nil {
		return fmt.Errorf("onSettingsCommand err = pubSub is nil")
	}
	v, ok := pubSub.(*v2.Settings_Publishing)
	if !ok {
		return fmt.Errorf("[bug] Issued settings not match with the client type, clientId=%v, clientType=%v", ps.GetClientID(), ps.GetClientType())
	}
	if settings.GetBackoffPolicy() != nil {
		exponentialBackoff := settings.GetBackoffPolicy().GetExponentialBackoff()
		if exponentialBackoff != nil {
			ps.retryPolicy.Strategy = &v2.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &v2.ExponentialBackoff{
					Max:        exponentialBackoff.GetMax(),
					Initial:    exponentialBackoff.GetInitial(),
					Multiplier: exponentialBackoff.GetMultiplier(),
				},
			}
		}
	}
	ps.validateMessageType = v.Publishing.GetValidateMessageType()
	ps.maxBodySizeBytes = int(v.Publishing.GetMaxBodySize())

	return nil
}
