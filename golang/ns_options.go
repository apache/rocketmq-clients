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
	"time"
)

type nameServerOptions struct {
	tickerDuration time.Duration
	timeout        time.Duration
	clientConnFunc ClientConnFunc
	connOptions    []ConnOption
	brokerFunc     BrokerFunc
	brokerOptions  []BrokerOption
}

var defaultNSOptions = nameServerOptions{
	tickerDuration: time.Second * 30,
	timeout:        time.Millisecond * 3000,
	clientConnFunc: NewClientConn,
	brokerFunc:     NewBroker,
}

// A NameServerOption sets options such as timeout, etc.
type NameServerOption interface {
	apply(*nameServerOptions)
}

// funcNSOption wraps a function that modifies options into an implementation of
// the NameServerOption interface.
type funcNSOption struct {
	f func(options *nameServerOptions)
}

func (fpo *funcNSOption) apply(po *nameServerOptions) {
	fpo.f(po)
}

func newFuncNSOption(f func(options *nameServerOptions)) *funcNSOption {
	return &funcNSOption{
		f: f,
	}
}

// WithQueryRouteTimeout returns a Option that sets timeout duration for nameserver.
// Default is 3s.
func WithQueryRouteTimeout(d time.Duration) NameServerOption {
	return newFuncNSOption(func(o *nameServerOptions) {
		o.timeout = d
	})
}

// WithTickerDuration returns a Option that sets ticker duration for nameserver.
// Default is 30s.
func WithTickerDuration(d time.Duration) NameServerOption {
	return newFuncNSOption(func(o *nameServerOptions) {
		o.tickerDuration = d
	})
}

// WithClientConnFunc returns a Option that sets ClientConnFunc for nameserver.
// Default is NewClientConn.
func WithClientConnFunc(f ClientConnFunc) NameServerOption {
	return newFuncNSOption(func(o *nameServerOptions) {
		o.clientConnFunc = f
	})
}

// WithBrokerFunc returns a Option that sets BrokerFunc for nameserver.
// Default is NewClient.
func WithBrokerFunc(f BrokerFunc) NameServerOption {
	return newFuncNSOption(func(o *nameServerOptions) {
		o.brokerFunc = f
	})
}

// WithConnOptions returns a Option that sets ConnOption for grpc ClientConn.
func WithConnOptions(opts ...ConnOption) NameServerOption {
	return newFuncNSOption(func(o *nameServerOptions) {
		o.connOptions = append(o.connOptions, opts...)
	})
}

// WithBrokerOptions returns a Option that sets BrokerOption for grpc ClientConn.
func WithBrokerOptions(opts ...BrokerOption) NameServerOption {
	return newFuncNSOption(func(o *nameServerOptions) {
		o.brokerOptions = append(o.brokerOptions, opts...)
	})
}
