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
	"math/rand"
	"time"

	"github.com/apache/rocketmq-clients/golang/metadata"
	"github.com/lithammer/shortuuid/v4"
)

type brokerOptions struct {
	id                  string
	producer            bool
	heartbeatDuration   time.Duration
	healthCheckDuration time.Duration
	timeout             time.Duration
	clientConnFunc      ClientConnFunc
	connOptions         []ConnOption
}

var defaultBrokerOptions = brokerOptions{
	id:                  fmt.Sprintf("%s@%d@%s", metadata.Rocketmq, rand.Int(), shortuuid.New()),
	heartbeatDuration:   time.Second * 10,
	healthCheckDuration: time.Second * 15,
	timeout:             time.Second * 5,
	clientConnFunc:      NewClientConn,
}

// A BrokerOption sets options such as tls.Config, etc.
type BrokerOption interface {
	apply(*brokerOptions)
}

// funcBrokerOption wraps a function that modifies options into an implementation of
// the ConnOption interface.
type funcBrokerOption struct {
	f func(options *brokerOptions)
}

func (fpo *funcBrokerOption) apply(po *brokerOptions) {
	fpo.f(po)
}

func newFuncOption(f func(options *brokerOptions)) *funcBrokerOption {
	return &funcBrokerOption{
		f: f,
	}
}

// WithBrokerClientConnFunc returns a BrokerOption that sets ClientConnFunc for Broker.
// Default is NewClientConn.
func WithBrokerClientConnFunc(f ClientConnFunc) BrokerOption {
	return newFuncOption(func(o *brokerOptions) {
		o.clientConnFunc = f
	})
}

// WithBrokerConnOption returns a BrokerOption that sets ConnOption for Broker.
func WithBrokerConnOption(opts ...ConnOption) BrokerOption {
	return newFuncOption(func(o *brokerOptions) {
		o.connOptions = append(o.connOptions, opts...)
	})
}

// WithProducer returns a BrokerOption that sets producer flag for Broker.
func WithProducer() BrokerOption {
	return newFuncOption(func(o *brokerOptions) {
		o.producer = true
	})
}

// WithHeartbeatDuration returns a BrokerOption that sets heartbeatDuration for Broker.
// Default is 10s.
func WithHeartbeatDuration(d time.Duration) BrokerOption {
	return newFuncOption(func(o *brokerOptions) {
		o.heartbeatDuration = d
	})
}

// WithHealthCheckDuration returns a BrokerOption that sets healthCheckDuration for Broker.
// Default is 15s.
func WithHealthCheckDuration(d time.Duration) BrokerOption {
	return newFuncOption(func(o *brokerOptions) {
		o.healthCheckDuration = d
	})
}

// WithBrokerTimeout returns a BrokerOption that sets time for Broker when heartbeat and health check.
// Default is 5s.
func WithBrokerTimeout(d time.Duration) BrokerOption {
	return newFuncOption(func(o *brokerOptions) {
		o.timeout = d
	})
}
